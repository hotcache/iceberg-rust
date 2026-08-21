// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shared engine for snapshot-producing operations that both add and delete files.
//!
//! [`MergingSnapshotProducer`] is the Rust equivalent of Java's
//! `MergingSnapshotProducer`. It handles manifest filtering, new manifest
//! creation, summary computation, and delegates the final snapshot commit to
//! [`SnapshotProducer`].

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use uuid::Uuid;

use crate::error::Result;
use crate::io::OutputFile;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, ManifestWriter, ManifestWriterBuilder, Operation, PartitionSpec, SchemaRef,
    SnapshotSummaryCollector, Summary, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::snapshot::SnapshotProducer;
use crate::{Error, ErrorKind};

/// Create a manifest writer that handles encryption when available.
fn new_manifest_writer(
    table: &Table,
    output_file: OutputFile,
    snapshot_id: Option<i64>,
    content: ManifestContentType,
    schema: SchemaRef,
    partition_spec: PartitionSpec,
) -> Result<ManifestWriter> {
    let builder = if let Some(em) = table.encryption_manager() {
        ManifestWriterBuilder::new_from_encrypted(
            em.encrypt(output_file),
            snapshot_id,
            schema,
            partition_spec,
        )?
    } else {
        ManifestWriterBuilder::new(output_file, snapshot_id, schema, partition_spec)
    };

    match table.metadata().format_version() {
        FormatVersion::V1 => Ok(builder.build_v1()),
        FormatVersion::V2 => match content {
            ManifestContentType::Data => Ok(builder.build_v2_data()),
            ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
        },
        FormatVersion::V3 => match content {
            ManifestContentType::Data => Ok(builder.build_v3_data()),
            ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
        },
    }
}

/// Filters existing manifests by removing entries for deleted data files.
///
/// This is equivalent to Java's `ManifestFilterManager`. When a rewrite or
/// overwrite operation deletes files, the filter manager rewrites affected
/// manifests so that deleted entries are dropped and surviving entries are
/// re-emitted with [`ManifestStatus::Existing`].
pub(crate) struct ManifestFilterManager {
    deleted_file_paths: HashSet<String>,
    fail_missing_delete_paths: bool,
}

impl ManifestFilterManager {
    pub(crate) fn new(fail_missing_delete_paths: bool) -> Self {
        Self {
            deleted_file_paths: HashSet::new(),
            fail_missing_delete_paths,
        }
    }

    pub(crate) fn add_delete(&mut self, path: String) {
        self.deleted_file_paths.insert(path);
    }

    /// Filter `manifests` by removing entries whose file path is in the delete
    /// set. Returns the surviving manifests, a [`SnapshotSummaryCollector`]
    /// that recorded metrics for every removed file, and the paths of any
    /// newly written manifest files (for orphan cleanup on retry).
    pub(crate) async fn filter_manifests(
        &self,
        table: &Table,
        manifests: Vec<ManifestFile>,
        snapshot_id: i64,
    ) -> Result<(Vec<ManifestFile>, SnapshotSummaryCollector, Vec<String>)> {
        if self.deleted_file_paths.is_empty() {
            return Ok((manifests, SnapshotSummaryCollector::default(), Vec::new()));
        }

        let mut result: Vec<ManifestFile> = Vec::with_capacity(manifests.len());
        let mut removed_collector = SnapshotSummaryCollector::default();
        let mut written_manifest_paths: Vec<String> = Vec::new();
        let mut found_paths: HashSet<String> = HashSet::new();

        for manifest_file in &manifests {
            // Only filter data manifests; pass delete manifests through unchanged.
            if manifest_file.content != ManifestContentType::Data {
                result.push(manifest_file.clone());
                continue;
            }

            let manifest = table.manifest_reader().read(manifest_file).await?;

            // Check whether this manifest contains any files we want to delete.
            let has_deletes = manifest
                .entries()
                .iter()
                .any(|e| e.is_alive() && self.deleted_file_paths.contains(e.file_path()));

            if !has_deletes {
                // Manifest is unaffected — pass through verbatim.
                result.push(manifest_file.clone());
                continue;
            }

            // Resolve the partition spec for this manifest. After partition
            // evolution, old manifests may carry a non-default spec — we must
            // use the manifest's own spec so the rewritten manifest stays valid.
            let manifest_spec = table
                .metadata()
                .partition_spec_by_id(manifest_file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Manifest references unknown partition spec {}",
                            manifest_file.partition_spec_id,
                        ),
                    )
                })?
                .clone();
            let schema = table.metadata().current_schema().clone();

            // Rewrite: keep surviving entries as EXISTING, drop deleted ones.
            let mut surviving_entries: Vec<ManifestEntry> = Vec::new();
            for entry in manifest.entries() {
                if entry.is_alive() && self.deleted_file_paths.contains(entry.file_path()) {
                    // Record removal metrics.
                    found_paths.insert(entry.file_path().to_string());
                    removed_collector.remove_file(
                        entry.data_file(),
                        schema.clone(),
                        manifest_spec.clone(),
                    );
                } else if entry.is_alive() {
                    // Surviving entry — re-emit as EXISTING with original ids preserved.
                    let existing = ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(
                            entry
                                .snapshot_id()
                                .unwrap_or(manifest_file.added_snapshot_id),
                        )
                        .sequence_number(entry.sequence_number().unwrap_or(0))
                        .file_sequence_number(entry.file_sequence_number.unwrap_or(0))
                        .data_file(entry.data_file().clone())
                        .build();
                    surviving_entries.push(existing);
                }
                // Already-deleted entries (status == Deleted) are dropped.
            }

            if surviving_entries.is_empty() {
                // Manifest is now empty — omit entirely.
                continue;
            }

            // Write the filtered manifest using the manifest's own partition spec.
            let new_manifest_path = format!(
                "{}/{}-m-filter-{}.{}",
                table.metadata().metadata_location()?,
                Uuid::now_v7(),
                manifest_file.partition_spec_id,
                DataFileFormat::Avro,
            );
            let output_file = table.file_io().new_output(new_manifest_path)?;
            // Use the current snapshot_id so that the manifest list writer
            // can assign sequence numbers to this rewritten manifest.
            let mut writer = new_manifest_writer(
                table,
                output_file,
                Some(snapshot_id),
                ManifestContentType::Data,
                schema.clone(),
                manifest_spec.as_ref().clone(),
            )?;
            for entry in surviving_entries {
                writer.add_entry(entry)?;
            }
            let new_manifest = writer.write_manifest_file().await?;
            written_manifest_paths.push(new_manifest.manifest_path.clone());
            result.push(new_manifest);
        }

        // Validate that every delete target was found.
        if self.fail_missing_delete_paths {
            let missing: Vec<&str> = self
                .deleted_file_paths
                .iter()
                .filter(|p| !found_paths.contains(p.as_str()))
                .map(String::as_str)
                .collect();
            if !missing.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Failed to find the following files to delete in the current snapshot: {}",
                        missing.join(", "),
                    ),
                ));
            }
        }

        Ok((result, removed_collector, written_manifest_paths))
    }
}

/// Shared engine for operations that both add and remove data files.
///
/// This struct is the Rust equivalent of Java's `MergingSnapshotProducer`.
/// It manages:
/// - Tracking files to add and delete
/// - Filtering existing manifests to remove deleted files
/// - Writing new manifests for added files
/// - Computing snapshot summaries that account for both additions and removals
/// - Delegating final snapshot creation to [`SnapshotProducer`]
///
/// Concrete transaction actions like [`RewriteFilesAction`] own an instance
/// of this struct and configure it with the appropriate [`Operation`] type
/// and validation rules.
pub(crate) struct MergingSnapshotProducer {
    operation: Operation,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    /// Delete files (position/equality) to add to the snapshot.
    /// These are written to a separate delete manifest.
    added_delete_files: Vec<DataFile>,
    filter_manager: ManifestFilterManager,
    commit_uuid: Uuid,
    /// Cache that survives across commit retries.
    ///
    /// When a commit fails due to a concurrent modification and the
    /// transaction retries, the manifests for *added* files don't change —
    /// only the filtering of *existing* manifests needs to be redone
    /// (because the base snapshot changed). Caching the added-file
    /// manifests avoids rewriting them on every attempt.
    ///
    /// The snapshot_id is also cached so that the manifest list writer
    /// can correctly assign sequence numbers to cached manifests.
    ///
    /// This uses `Mutex` for interior mutability because
    /// `TransactionAction::commit` receives `self: Arc<Self>`, requiring
    /// shared-reference mutation. The lock is held only for brief
    /// cache reads/writes — never across `.await` points.
    cache: Mutex<ManifestCache>,
}

/// Cached state that persists across commit retries.
#[derive(Default)]
struct ManifestCache {
    /// Snapshot ID generated on the first attempt; reused on retries so
    /// that cached manifests (which carry this ID) remain valid.
    snapshot_id: Option<i64>,
    /// Manifests written for newly added data files. These are
    /// content-stable across retries and can be reused as-is.
    new_data_manifests: Option<Vec<ManifestFile>>,
    /// Manifests written for newly added delete files.
    new_delete_manifests: Option<Vec<ManifestFile>>,
    /// Paths of filtered manifest files written during the previous
    /// attempt. On retry, these are deleted before writing new ones
    /// to avoid orphaned files in storage.
    previous_filter_manifest_paths: Vec<String>,
}

impl MergingSnapshotProducer {
    pub(crate) fn new(operation: Operation) -> Self {
        Self {
            operation,
            added_data_files: Vec::new(),
            deleted_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            filter_manager: ManifestFilterManager::new(true),
            commit_uuid: Uuid::now_v7(),
            cache: Mutex::new(ManifestCache::default()),
        }
    }

    pub(crate) fn add_data_file(&mut self, file: DataFile) {
        self.added_data_files.push(file);
    }

    /// Add a delete file (position or equality) to the snapshot.
    pub(crate) fn add_delete_file(&mut self, file: DataFile) {
        self.added_delete_files.push(file);
    }

    pub(crate) fn delete_data_file(&mut self, file: DataFile) {
        self.filter_manager.add_delete(file.file_path.clone());
        self.deleted_data_files.push(file);
    }

    pub(crate) fn has_added_data_files(&self) -> bool {
        !self.added_data_files.is_empty()
    }

    pub(crate) fn has_deleted_data_files(&self) -> bool {
        !self.deleted_data_files.is_empty()
    }

    pub(crate) fn deleted_data_files(&self) -> &[DataFile] {
        &self.deleted_data_files
    }

    pub(crate) fn has_added_delete_files(&self) -> bool {
        !self.added_delete_files.is_empty()
    }

    pub(crate) fn added_delete_files(&self) -> &[DataFile] {
        &self.added_delete_files
    }

    /// Produce manifests, compute summary, and commit a new snapshot
    /// using the operation type specified at construction time.
    pub(crate) async fn commit_snapshot(&self, table: &Table) -> Result<ActionCommit> {
        self.commit_snapshot_with_operation(table, self.operation.clone())
            .await
    }

    /// Produce manifests, compute summary, and commit a new snapshot
    /// with the given operation type.
    ///
    /// On the first call, this writes new manifests for added files and
    /// caches them. On subsequent calls (retries), the cached manifests
    /// are reused while existing-manifest filtering is always redone
    /// (because the base snapshot may have changed).
    ///
    /// The `operation` parameter allows callers like [`OverwriteFilesAction`]
    /// to determine the operation type dynamically.
    pub(crate) async fn commit_snapshot_with_operation(
        &self,
        table: &Table,
        operation: Operation,
    ) -> Result<ActionCommit> {
        let mut snapshot_producer =
            SnapshotProducer::new(table, self.commit_uuid, HashMap::new(), Vec::new());

        // Snapshot the cache state in a single lock acquisition. This
        // avoids multiple lock/unlock cycles and keeps the critical section
        // brief (no `.await` while locked).
        let (snapshot_id, cached_manifests) = {
            let mut cache = self.cache.lock().expect("cache lock poisoned");
            let snapshot_id = match cache.snapshot_id {
                Some(id) => {
                    snapshot_producer.snapshot_id = id;
                    id
                }
                None => {
                    let id = snapshot_producer.snapshot_id;
                    cache.snapshot_id = Some(id);
                    id
                }
            };
            (snapshot_id, cache.new_data_manifests.clone())
        };

        // 1. Load existing manifests from the current snapshot.
        let existing_manifests = match table.metadata().current_snapshot() {
            Some(snapshot) => {
                let manifest_list = table.manifest_list_reader(snapshot).load().await?;
                manifest_list
                    .entries()
                    .iter()
                    .filter(|e| {
                        e.has_added_files() || e.has_existing_files() || e.has_deleted_files()
                    })
                    .cloned()
                    .collect()
            }
            None => Vec::new(),
        };

        // 2. Clean up filtered manifests from any previous attempt, then
        //    redo filtering (the base snapshot may have changed after a retry).
        let orphaned_paths: Vec<String> = {
            let mut cache = self.cache.lock().expect("cache lock poisoned");
            cache.previous_filter_manifest_paths.drain(..).collect()
        };
        for path in &orphaned_paths {
            if let Err(e) = table.file_io().delete(path).await {
                tracing::warn!("Failed to clean up orphaned filtered manifest {}: {}", path, e);
            }
        }
        let (mut filtered_manifests, removed_collector, filter_written_paths) = self
            .filter_manager
            .filter_manifests(table, existing_manifests, snapshot_id)
            .await?;
        // Track written paths so we can clean them up on the next retry.
        {
            let mut cache = self.cache.lock().expect("cache lock poisoned");
            cache.previous_filter_manifest_paths = filter_written_paths;
        }

        // 3. Get cached or write new manifests for added files.
        //    Added files don't change between retries, so their manifests
        //    can be safely reused.
        if !self.added_data_files.is_empty() {
            let added_manifests = match cached_manifests {
                Some(manifests) => manifests,
                None => {
                    let manifest = self
                        .write_manifest_for_files(
                            table,
                            snapshot_id,
                            ManifestContentType::Data,
                            &self.added_data_files,
                            "added",
                        )
                        .await?;
                    let manifests = vec![manifest];
                    self.cache
                        .lock()
                        .expect("cache lock poisoned")
                        .new_data_manifests = Some(manifests.clone());
                    manifests
                }
            };
            filtered_manifests.extend(added_manifests);
        }

        // 3b. Get cached or write new manifests for added delete files.
        if !self.added_delete_files.is_empty() {
            let cached = {
                let cache = self.cache.lock().expect("cache lock poisoned");
                cache.new_delete_manifests.clone()
            };
            let delete_manifests = match cached {
                Some(manifests) => manifests,
                None => {
                    let manifest = self
                        .write_manifest_for_files(
                            table,
                            snapshot_id,
                            ManifestContentType::Deletes,
                            &self.added_delete_files,
                            "deletes",
                        )
                        .await?;
                    let manifests = vec![manifest];
                    let mut cache = self.cache.lock().expect("cache lock poisoned");
                    cache.new_delete_manifests = Some(manifests.clone());
                    manifests
                }
            };
            filtered_manifests.extend(delete_manifests);
        }

        // 4. Compute summary (added + removed).
        let summary = self.build_summary(table, removed_collector, &operation)?;

        // 5. Delegate to SnapshotProducer for manifest list + snapshot creation.
        snapshot_producer
            .commit_with_manifests(filtered_manifests, summary)
            .await
    }

    /// Commit a snapshot where delete files are determined dynamically
    /// (e.g., by a row filter) on each attempt.
    ///
    /// Unlike [`commit_snapshot_with_operation`], this method accepts
    /// delete files as a parameter rather than using the internal
    /// `filter_manager`. This allows the caller to re-evaluate which
    /// files to delete on each retry while still reusing the cached
    /// added-file manifests.
    pub(crate) async fn commit_snapshot_with_dynamic_deletes(
        &self,
        table: &Table,
        operation: Operation,
        delete_files: Vec<DataFile>,
    ) -> Result<ActionCommit> {
        let mut snapshot_producer =
            SnapshotProducer::new(table, self.commit_uuid, HashMap::new(), Vec::new());

        let (snapshot_id, cached_manifests) = {
            let mut cache = self.cache.lock().expect("cache lock poisoned");
            let snapshot_id = match cache.snapshot_id {
                Some(id) => {
                    snapshot_producer.snapshot_id = id;
                    id
                }
                None => {
                    let id = snapshot_producer.snapshot_id;
                    cache.snapshot_id = Some(id);
                    id
                }
            };
            (snapshot_id, cache.new_data_manifests.clone())
        };

        // 1. Load existing manifests.
        let existing_manifests = match table.metadata().current_snapshot() {
            Some(snapshot) => {
                let manifest_list = table.manifest_list_reader(snapshot).load().await?;
                manifest_list
                    .entries()
                    .iter()
                    .filter(|e| {
                        e.has_added_files() || e.has_existing_files() || e.has_deleted_files()
                    })
                    .cloned()
                    .collect()
            }
            None => Vec::new(),
        };

        // 2. Clean up filtered manifests from any previous attempt, then
        //    build a temporary filter manager for the dynamic deletes.
        let orphaned_paths: Vec<String> = {
            let mut cache = self.cache.lock().expect("cache lock poisoned");
            cache.previous_filter_manifest_paths.drain(..).collect()
        };
        for path in &orphaned_paths {
            if let Err(e) = table.file_io().delete(path).await {
                tracing::warn!("Failed to clean up orphaned filtered manifest {}: {}", path, e);
            }
        }
        let mut filter = ManifestFilterManager::new(true);
        for f in &delete_files {
            filter.add_delete(f.file_path.clone());
        }
        let (mut filtered_manifests, removed_collector, filter_written_paths) = filter
            .filter_manifests(table, existing_manifests, snapshot_id)
            .await?;
        // Track written paths so we can clean them up on the next retry.
        {
            let mut cache = self.cache.lock().expect("cache lock poisoned");
            cache.previous_filter_manifest_paths = filter_written_paths;
        }

        // 3. Reuse cached added-file manifests.
        if !self.added_data_files.is_empty() {
            let added_manifests = match cached_manifests {
                Some(manifests) => manifests,
                None => {
                    let manifest = self
                        .write_manifest_for_files(
                            table,
                            snapshot_id,
                            ManifestContentType::Data,
                            &self.added_data_files,
                            "added",
                        )
                        .await?;
                    let manifests = vec![manifest];
                    self.cache
                        .lock()
                        .expect("cache lock poisoned")
                        .new_data_manifests = Some(manifests.clone());
                    manifests
                }
            };
            filtered_manifests.extend(added_manifests);
        }

        // 4. Compute summary including dynamic deletes.
        let table_metadata = table.metadata_ref();
        let schema = table_metadata.current_schema().clone();
        let partition_spec = table_metadata.default_partition_spec().clone();

        let mut collector = SnapshotSummaryCollector::default();
        for file in &self.added_data_files {
            collector.add_file(file, schema.clone(), partition_spec.clone());
        }
        collector.merge(removed_collector);

        let summary = Summary {
            operation: operation.clone(),
            additional_properties: collector.build(),
        };

        let previous_snapshot = table_metadata.current_snapshot();
        let summary =
            update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)?;

        // 5. Commit.
        snapshot_producer
            .commit_with_manifests(filtered_manifests, summary)
            .await
    }

    /// Write a manifest file for the given files and content type.
    async fn write_manifest_for_files(
        &self,
        table: &Table,
        snapshot_id: i64,
        content: ManifestContentType,
        files: &[DataFile],
        path_suffix: &str,
    ) -> Result<ManifestFile> {
        if content == ManifestContentType::Deletes
            && table.metadata().format_version() == FormatVersion::V1
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Delete files are not supported in format version 1",
            ));
        }

        let new_manifest_path = format!(
            "{}/{}-m-{}.{}",
            table.metadata().metadata_location()?,
            self.commit_uuid,
            path_suffix,
            DataFileFormat::Avro,
        );
        let output_file = table.file_io().new_output(new_manifest_path)?;
        let schema = table.metadata().current_schema().clone();
        let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

        let mut writer = new_manifest_writer(
            table,
            output_file,
            Some(snapshot_id),
            content,
            schema,
            partition_spec,
        )?;

        let format_version = table.metadata().format_version();
        for file in files {
            let entry_builder = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(file.clone());
            let entry = if format_version == FormatVersion::V1 {
                // V1 requires snapshot_id on each entry.
                entry_builder.snapshot_id(snapshot_id).build()
            } else {
                entry_builder.build()
            };
            writer.add_entry(entry)?;
        }

        writer.write_manifest_file().await
    }

    fn build_summary(
        &self,
        table: &Table,
        removed_collector: SnapshotSummaryCollector,
        operation: &Operation,
    ) -> Result<Summary> {
        let table_metadata = table.metadata_ref();
        let schema = table_metadata.current_schema().clone();
        let partition_spec = table_metadata.default_partition_spec().clone();

        let mut collector = SnapshotSummaryCollector::default();
        for file in &self.added_data_files {
            collector.add_file(file, schema.clone(), partition_spec.clone());
        }
        for file in &self.added_delete_files {
            collector.add_file(file, schema.clone(), partition_spec.clone());
        }
        // Merge removal metrics from the filter manager.
        collector.merge(removed_collector);

        let summary = Summary {
            operation: operation.clone(),
            additional_properties: collector.build(),
        };

        let previous_snapshot = table_metadata.current_snapshot();
        update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)
    }
}
