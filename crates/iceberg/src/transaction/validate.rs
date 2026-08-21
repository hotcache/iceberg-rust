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

//! Snapshot validation utilities for conflict detection.
//!
//! When multiple writers concurrently modify a table, a rewrite or overwrite
//! operation must verify that no conflicting changes were made between the
//! time it read the table state and the time it commits. This module provides
//! the building blocks for those checks.

use std::collections::HashSet;

use crate::error::Result;
use crate::spec::{DataFile, ManifestContentType, Operation, TableMetadata};
use crate::table::Table;
use crate::{Error, ErrorKind};

/// Walk the snapshot ancestry from `current_snapshot_id` back to
/// `starting_snapshot_id` (exclusive) and find snapshots whose operation
/// may have introduced new delete files.
///
/// Returns the snapshot IDs of intervening snapshots that used
/// [`Operation::Overwrite`] or [`Operation::Delete`].
fn find_snapshots_with_deletes(
    metadata: &TableMetadata,
    starting_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> Vec<i64> {
    let Some(current_id) = current_snapshot_id else {
        return Vec::new();
    };

    let adds_deletes = |op: &Operation| matches!(op, Operation::Overwrite | Operation::Delete);

    let mut result = Vec::new();
    let mut snapshot_id = Some(current_id);

    while let Some(id) = snapshot_id {
        if Some(id) == starting_snapshot_id {
            break;
        }

        let Some(snapshot) = metadata.snapshot_by_id(id) else {
            break;
        };

        if adds_deletes(&snapshot.summary().operation) {
            result.push(id);
        }

        snapshot_id = snapshot.parent_snapshot_id();
    }

    result
}

/// Validate that no new delete files have been added for the given data files
/// since `starting_snapshot_id`.
///
/// This is the Rust equivalent of Java's `validateNoNewDeletesForDataFiles`.
/// It walks the snapshot ancestry from the current snapshot back to the
/// starting snapshot and checks whether any new position-delete files
/// reference the data files being rewritten.
///
/// # Arguments
///
/// * `table` — the current table state (may differ from when the operation started)
/// * `starting_snapshot_id` — the snapshot at which the operation began reading;
///   only changes *after* this snapshot are checked
/// * `replaced_data_files` — the data files being replaced by the rewrite
pub(crate) async fn validate_no_new_deletes_for_data_files(
    table: &Table,
    starting_snapshot_id: Option<i64>,
    replaced_data_files: &[DataFile],
) -> Result<()> {
    if replaced_data_files.is_empty() {
        return Ok(());
    }

    let metadata = table.metadata();
    let current_snapshot_id = metadata.current_snapshot_id();

    // If the snapshot hasn't changed, there's nothing to validate.
    if current_snapshot_id == starting_snapshot_id {
        return Ok(());
    }

    let snapshot_ids_with_deletes =
        find_snapshots_with_deletes(metadata, starting_snapshot_id, current_snapshot_id);

    if snapshot_ids_with_deletes.is_empty() {
        return Ok(());
    }

    // Build a set of paths for the files being replaced.
    let replaced_paths: HashSet<&str> = replaced_data_files
        .iter()
        .map(|f| f.file_path.as_str())
        .collect();

    // For each snapshot that may have added deletes, load its delete manifests
    // and check for conflicts.
    for snap_id in &snapshot_ids_with_deletes {
        let Some(snapshot) = metadata.snapshot_by_id(*snap_id) else {
            continue;
        };

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;

        for manifest_file in manifest_list.entries() {
            // Only check delete manifests added in this snapshot.
            if manifest_file.content != ManifestContentType::Deletes
                || manifest_file.added_snapshot_id != *snap_id
            {
                continue;
            }

            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                // Position delete files carry a `referenced_data_file` that
                // tells us which data file they target.
                if let Some(ref referenced) = entry.data_file().referenced_data_file
                    && replaced_paths.contains(referenced.as_str())
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot commit, found new position delete for replaced data file: {}",
                            referenced,
                        ),
                    ));
                }
                // Equality deletes without a specific referenced file use a
                // partition-based approach that is more complex. Deferred to
                // a follow-up — matches Java's incremental approach.
            }
        }
    }

    Ok(())
}

/// Validate that the data files referenced by delete files still exist in
/// the current snapshot.
///
/// This is used by [`RowDeltaAction`] to ensure that position delete files
/// don't reference data files that have been concurrently removed (e.g.,
/// by a compaction or overwrite).
///
/// # Arguments
///
/// * `table` — the current table state
/// * `referenced_data_file_paths` — paths of data files that delete files
///   reference (from `DataFile::referenced_data_file`)
pub(crate) async fn validate_data_files_exist(
    table: &Table,
    referenced_data_file_paths: &[String],
) -> Result<()> {
    if referenced_data_file_paths.is_empty() {
        return Ok(());
    }

    let Some(snapshot) = table.metadata().current_snapshot() else {
        // No snapshot — no data files can exist.
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot add delete files: table has no snapshot, but delete files reference: {}",
                referenced_data_file_paths.join(", "),
            ),
        ));
    };

    // Collect all live data file paths from the current snapshot.
    let manifest_list = table.manifest_list_reader(snapshot).load().await?;
    let mut live_paths: HashSet<String> = HashSet::new();

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if entry.is_alive() {
                live_paths.insert(entry.file_path().to_string());
            }
        }
    }

    let missing: Vec<&str> = referenced_data_file_paths
        .iter()
        .filter(|p| !live_paths.contains(p.as_str()))
        .map(String::as_str)
        .collect();

    if !missing.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot add delete files that reference missing data files: {}",
                missing.join(", "),
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::tests::new_memory_catalog;
    use crate::transaction::tests::{
        append_files, make_data_file, make_position_delete_file, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};

    /// Snapshot unchanged since start → validation passes.
    #[tokio::test]
    async fn test_validate_no_change_since_start() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        let starting = table.metadata().current_snapshot_id();
        let result = validate_no_new_deletes_for_data_files(&table, starting, &[f1]).await;
        assert!(result.is_ok());
    }

    /// Concurrent appends (no deletes) → validation passes.
    #[tokio::test]
    async fn test_validate_passes_with_concurrent_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let starting = table.metadata().current_snapshot_id();

        // Another append — Operation::Append doesn't introduce delete files.
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f2]).await;

        let result = validate_no_new_deletes_for_data_files(&table, starting, &[f1]).await;
        assert!(result.is_ok());
    }

    /// Empty replaced files → validation passes.
    #[tokio::test]
    async fn test_validate_empty_replaced_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let result = validate_no_new_deletes_for_data_files(&table, None, &[]).await;
        assert!(result.is_ok());
    }

    /// Concurrent RowDelta adds a position delete targeting a file we're
    /// replacing → validation fails.
    #[tokio::test]
    async fn test_validate_fails_with_concurrent_position_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append data file.
        let f1 = make_data_file(&table, "test/1.parquet", 100, 1000);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let starting = table.metadata().current_snapshot_id();

        // Concurrent RowDelta: someone adds a position delete targeting f1.
        let del = make_position_delete_file(&table, "test/del-1.parquet", 5, "test/1.parquet");
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(del);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Now try to validate rewriting f1 — should fail because a new
        // position delete was added for f1 since starting_snapshot_id.
        let result = validate_no_new_deletes_for_data_files(&table, starting, &[f1]).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("found new position delete for replaced data file"),
            "unexpected error: {}",
            err.message()
        );
    }

    /// Concurrent RowDelta adds a position delete for a DIFFERENT file →
    /// validation passes.
    #[tokio::test]
    async fn test_validate_passes_when_delete_targets_other_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 100, 1000);
        let f2 = make_data_file(&table, "test/2.parquet", 100, 1000);
        let table = append_files(&catalog, &table, vec![f1.clone(), f2]).await;
        let starting = table.metadata().current_snapshot_id();

        // Someone adds a position delete targeting f2, not f1.
        let del = make_position_delete_file(&table, "test/del-2.parquet", 5, "test/2.parquet");
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(del);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Validate rewriting f1 — should pass because the delete targets f2.
        let result = validate_no_new_deletes_for_data_files(&table, starting, &[f1]).await;
        assert!(result.is_ok());
    }

    /// validate_data_files_exist: all referenced files exist → passes.
    #[tokio::test]
    async fn test_validate_data_files_exist_passes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1]).await;

        let result = validate_data_files_exist(&table, &["test/1.parquet".to_string()]).await;
        assert!(result.is_ok());
    }

    /// validate_data_files_exist: referenced file missing → fails.
    #[tokio::test]
    async fn test_validate_data_files_exist_fails_for_missing() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1]).await;

        let result = validate_data_files_exist(&table, &["test/ghost.parquet".to_string()]).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("reference missing data files"),
        );
    }
}
