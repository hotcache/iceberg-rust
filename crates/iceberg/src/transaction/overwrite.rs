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

//! Transaction action for overwriting data files.
//!
//! [`OverwriteFilesAction`] replaces data files with new ones, changing the
//! logical contents of the table.
//!
//! Two modes are supported:
//!
//! - **Explicit file mode** ([`delete_file`] + [`add_file`]): Deletes specific
//!   files by path. Not retry-safe for concurrent modifications.
//!
//! - **Row-filter mode** ([`overwrite_by_row_filter`] + [`add_file`]): Deletes
//!   files matching an expression. Re-evaluated on each retry attempt, making
//!   it safe for concurrent modifications. Equivalent to Java's
//!   `overwriteByRowFilter(Expression)`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::strict_metrics_evaluator::StrictMetricsEvaluator;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{DataFile, ManifestContentType, Operation, Schema};
use crate::table::Table;
use crate::transaction::merging::MergingSnapshotProducer;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// A transaction action that overwrites data files.
///
/// This is the Rust equivalent of Java's `BaseOverwriteFiles`.
///
/// # Known Limitations
///
/// - **No conflict detection** in explicit file mode: concurrent modifications
///   between the time files are read and committed are not detected.
///   Row-filter mode is retry-safe.
///
/// # Example — explicit file mode
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.overwrite_files()
///     .delete_file(old_file)
///     .add_file(new_file);
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
///
/// # Example — row-filter mode (retry-safe)
///
/// ```ignore
/// use iceberg::expr::Predicate;
///
/// let tx = Transaction::new(&table);
/// let action = tx.overwrite_files()
///     .overwrite_by_row_filter(Predicate::AlwaysTrue)
///     .add_file(new_file);
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
pub struct OverwriteFilesAction {
    /// The producer's `operation` field is not used directly — the
    /// operation type is determined dynamically at commit time based on
    /// which files were added/deleted.
    producer: MergingSnapshotProducer,
    /// When set, files matching this expression are dynamically deleted
    /// at commit time. Re-evaluated on each retry attempt.
    row_filter: Option<Predicate>,
}

impl OverwriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            // The operation here is a placeholder; the actual operation is
            // determined dynamically by `self.operation()` at commit time.
            producer: MergingSnapshotProducer::new(Operation::Overwrite),
            row_filter: None,
        }
    }

    /// Delete files whose rows match the given expression.
    ///
    /// This is the Rust equivalent of Java's `overwriteByRowFilter`. At commit
    /// time, the expression is bound to the table schema, projected onto the
    /// partition spec, and evaluated against each data file. Files where **all**
    /// rows match the filter are deleted.
    ///
    /// Because the filter is re-evaluated on each commit attempt, this mode is
    /// safe for concurrent modifications: if another writer changes the files
    /// between attempts, the filter picks up the new state.
    ///
    /// Use `Predicate::AlwaysTrue` to delete all existing data files.
    pub fn overwrite_by_row_filter(mut self, filter: Predicate) -> Self {
        self.row_filter = Some(filter);
        self
    }

    /// Register a data file to be removed from the table (explicit mode).
    pub fn delete_file(mut self, file: DataFile) -> Self {
        self.producer.delete_data_file(file);
        self
    }

    /// Register a data file to be added to the table.
    pub fn add_file(mut self, file: DataFile) -> Self {
        self.producer.add_data_file(file);
        self
    }

    fn validate(&self) -> Result<()> {
        if self.row_filter.is_some() && self.producer.has_deleted_data_files() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot use both overwrite_by_row_filter and delete_file in the same action",
            ));
        }
        if self.row_filter.is_none()
            && !self.producer.has_added_data_files()
            && !self.producer.has_deleted_data_files()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Overwrite requires a row filter, or at least one file to add or delete",
            ));
        }
        Ok(())
    }

    /// Scan the current snapshot and collect all data files whose rows match
    /// the given bound predicate. A file is selected for deletion only if
    /// **all** its rows match (strict evaluation). If some but not all rows
    /// match, an error is returned.
    async fn collect_files_matching_filter(
        table: &Table,
        bound_filter: &BoundPredicate,
        partition_evaluator: &ExpressionEvaluator,
    ) -> Result<Vec<DataFile>> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(Vec::new());
        };

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;
        let mut matched_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = table.manifest_reader().read(manifest_file).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }

                let data_file = entry.data_file();

                // Stage 1: partition evaluation (fast, no I/O).
                if !partition_evaluator.eval(data_file)? {
                    continue;
                }

                // Stage 2: strict metrics — do ALL rows match?
                let all_rows_match = StrictMetricsEvaluator::eval(bound_filter, data_file)?;

                if all_rows_match {
                    matched_files.push(data_file.clone());
                    continue;
                }

                // Stage 3: inclusive metrics — do SOME rows match?
                let some_rows_match =
                    InclusiveMetricsEvaluator::eval(bound_filter, data_file, true)?;

                if some_rows_match {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot delete file where some, but not all, rows match filter: {}",
                            data_file.file_path,
                        ),
                    ));
                }
                // No rows match → skip this file.
            }
        }

        Ok(matched_files)
    }
}

#[async_trait]
impl TransactionAction for OverwriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.validate()?;

        if let Some(ref row_filter) = self.row_filter {
            // Row-filter mode: dynamically find files to delete on each attempt.
            let schema = table.metadata().current_schema().clone();
            let partition_spec = table.metadata().default_partition_spec().clone();

            // Bind the filter to the schema.
            let bound_filter = row_filter.bind(schema.clone(), true)?;

            // Project to partition spec for fast partition-level filtering.
            // rewrite_not() removes NOT nodes, which ExpressionEvaluator
            // does not support.
            let mut projection = InclusiveProjection::new(partition_spec.clone());
            let partition_filter = projection.project(&bound_filter)?.rewrite_not();
            let partition_type = partition_spec.partition_type(&schema)?;
            let partition_schema = Arc::new(
                Schema::builder()
                    .with_schema_id(partition_spec.spec_id())
                    .with_fields(partition_type.fields().to_owned())
                    .build()?,
            );
            let bound_partition_filter = partition_filter.bind(partition_schema, true)?;
            let partition_evaluator = ExpressionEvaluator::new(bound_partition_filter);

            // Collect matching files from current snapshot.
            let delete_files =
                Self::collect_files_matching_filter(table, &bound_filter, &partition_evaluator)
                    .await?;

            let has_adds = self.producer.has_added_data_files();
            let has_deletes = !delete_files.is_empty();
            let operation = match (has_adds, has_deletes) {
                (true, true) => Operation::Overwrite,
                (false, true) => Operation::Delete,
                (true, false) => Operation::Append,
                (false, false) => Operation::Overwrite,
            };

            // Use the shared producer so added-file manifests are cached
            // across retry attempts. Only the delete set changes per attempt.
            self.producer
                .commit_snapshot_with_dynamic_deletes(table, operation, delete_files)
                .await
        } else {
            // Explicit file mode: use the fixed delete list with caching.
            let has_adds = self.producer.has_added_data_files();
            let has_deletes = self.producer.has_deleted_data_files();
            let operation = match (has_adds, has_deletes) {
                (true, true) => Operation::Overwrite,
                (false, true) => Operation::Delete,
                (true, false) => Operation::Append,
                (false, false) => unreachable!("validate() ensures at least one file exists"),
            };
            self.producer
                .commit_snapshot_with_operation(table, operation)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expr::Predicate;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::Operation;
    use crate::transaction::tests::{
        append_files, make_data_file, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};

    /// Overwrite: delete old files and add new ones → Operation::Overwrite.
    #[tokio::test]
    async fn test_overwrite_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        let f2 = make_data_file(&table, "test/2.parquet", 20, 200);
        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().delete_file(f1).add_file(f2);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-records").unwrap(), "20");
        assert_eq!(summary.get("added-data-files").unwrap(), "1");
        assert_eq!(summary.get("deleted-data-files").unwrap(), "1");
    }

    /// Delete only → Operation::Delete.
    #[tokio::test]
    async fn test_overwrite_delete_only() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().delete_file(f1);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Delete);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "0");
        assert_eq!(summary.get("total-records").unwrap(), "0");
        assert_eq!(summary.get("deleted-data-files").unwrap(), "1");
    }

    /// Add only → Operation::Append.
    #[tokio::test]
    async fn test_overwrite_add_only() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().add_file(f1);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Append);
    }

    /// Empty overwrite → error.
    #[tokio::test]
    async fn test_overwrite_empty() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let tx = Transaction::new(&table);
        let action = tx.overwrite_files();
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("row filter, or at least one file")
        );
    }

    /// Explicit mode: non-existent delete target → error.
    #[tokio::test]
    async fn test_overwrite_missing_delete_target() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1]).await;

        let ghost = make_data_file(&table, "test/ghost.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 20, 200);
        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().delete_file(ghost).add_file(f2);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("Failed to find the following files to delete")
        );
    }

    /// overwrite_by_row_filter(AlwaysTrue): deletes all existing files.
    #[tokio::test]
    async fn test_overwrite_by_row_filter_always_true() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 20, 200);
        let table = append_files(&catalog, &table, vec![f1, f2]).await;

        let f3 = make_data_file(&table, "test/3.parquet", 50, 500);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Predicate::AlwaysTrue)
            .add_file(f3);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-records").unwrap(), "50");
        assert_eq!(summary.get("deleted-data-files").unwrap(), "2");
    }

    /// overwrite_by_row_filter on empty table: just adds files.
    #[tokio::test]
    async fn test_overwrite_by_row_filter_empty_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Predicate::AlwaysTrue)
            .add_file(f1);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Append);
    }

    /// overwrite_by_row_filter with a column expression: only deletes files
    /// where ALL rows match the filter.
    ///
    /// Table schema: x (long, partition), y (long), z (long).
    /// Filter: x > 50
    ///
    /// file1: partition x=10, bounds x=[10,10] → no rows match → kept
    /// file2: partition x=60, bounds x=[60,60] → all rows match → deleted
    #[tokio::test]
    async fn test_overwrite_by_row_filter_column_expression() {
        use std::collections::HashMap;

        use crate::expr::Reference;
        use crate::spec::{DataFileBuilder, Datum};

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // file1: x=10 (all rows have x=10, which does NOT match x > 50)
        let f1 = DataFileBuilder::default()
            .content(crate::spec::DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(crate::spec::DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(crate::spec::Struct::from_iter([Some(
                crate::spec::Literal::long(10),
            )]))
            .partition_spec_id(0)
            .lower_bounds(HashMap::from([(1, Datum::long(10))]))
            .upper_bounds(HashMap::from([(1, Datum::long(10))]))
            .value_counts(HashMap::from([(1, 10)]))
            .null_value_counts(HashMap::from([(1, 0)]))
            .nan_value_counts(HashMap::from([(1, 0)]))
            .build()
            .unwrap();

        // file2: x=60 (all rows have x=60, which matches x > 50)
        let f2 = DataFileBuilder::default()
            .content(crate::spec::DataContentType::Data)
            .file_path("test/2.parquet".to_string())
            .file_format(crate::spec::DataFileFormat::Parquet)
            .file_size_in_bytes(200)
            .record_count(20)
            .partition(crate::spec::Struct::from_iter([Some(
                crate::spec::Literal::long(60),
            )]))
            .partition_spec_id(0)
            .lower_bounds(HashMap::from([(1, Datum::long(60))]))
            .upper_bounds(HashMap::from([(1, Datum::long(60))]))
            .value_counts(HashMap::from([(1, 20)]))
            .null_value_counts(HashMap::from([(1, 0)]))
            .nan_value_counts(HashMap::from([(1, 0)]))
            .build()
            .unwrap();

        let table = append_files(&catalog, &table, vec![f1, f2]).await;

        // Filter: x > 50 → should delete only file2.
        let filter = Reference::new("x").greater_than(Datum::long(50));
        let f3 = make_data_file(&table, "test/3.parquet", 5, 50);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(filter)
            .add_file(f3);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        let summary = &snapshot.summary().additional_properties;
        // file1 (kept) + file3 (added) = 2 files
        assert_eq!(summary.get("total-data-files").unwrap(), "2");
        // file1 (10 records) + file3 (5 records) = 15
        assert_eq!(summary.get("total-records").unwrap(), "15");
        // only file2 was deleted
        assert_eq!(summary.get("deleted-data-files").unwrap(), "1");
    }

    /// overwrite_by_row_filter with partial match → error.
    ///
    /// Uses a non-partition column (y) for the filter so that partition
    /// pruning doesn't skip the file. The file's y-column bounds span
    /// both sides of the filter threshold.
    ///
    /// file1: partition x=1, y bounds [30,70], filter y > 50
    ///   → partition eval passes (no pruning on y)
    ///   → strict metrics: lower_bound(y)=30, NOT > 50 → false
    ///   → inclusive metrics: upper_bound(y)=70, >= 50 → true
    ///   → partial match → error
    #[tokio::test]
    async fn test_overwrite_by_row_filter_partial_match_error() {
        use std::collections::HashMap;

        use crate::expr::Reference;
        use crate::spec::{DataFileBuilder, Datum};

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // file1: partition x=1, column y ranges from 30 to 70.
        // Filter y > 50: some rows match (y>50), some don't (y<=50).
        let f1 = DataFileBuilder::default()
            .content(crate::spec::DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(crate::spec::DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(crate::spec::Struct::from_iter([Some(
                crate::spec::Literal::long(1),
            )]))
            .partition_spec_id(0)
            .lower_bounds(HashMap::from([(2, Datum::long(30))]))
            .upper_bounds(HashMap::from([(2, Datum::long(70))]))
            .value_counts(HashMap::from([(2, 10)]))
            .null_value_counts(HashMap::from([(2, 0)]))
            .nan_value_counts(HashMap::from([(2, 0)]))
            .build()
            .unwrap();

        let table = append_files(&catalog, &table, vec![f1]).await;

        let filter = Reference::new("y").greater_than(Datum::long(50));
        let f2 = make_data_file(&table, "test/2.parquet", 5, 50);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(filter)
            .add_file(f2);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("some, but not all, rows match filter"),
            "expected partial match error"
        );
    }

    /// overwrite_by_row_filter is retry-safe: calling commit twice produces
    /// consistent results because the filter re-scans the current snapshot.
    #[tokio::test]
    async fn test_overwrite_by_row_filter_retry_safe() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1]).await;

        let f2 = make_data_file(&table, "test/2.parquet", 20, 200);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Predicate::AlwaysTrue)
            .add_file(f2);
        let action = Arc::new(action);

        // First call.
        let mut result1 = Arc::clone(&action).commit(&table).await.unwrap();
        let updates1 = result1.take_updates();

        // Second call (simulates retry).
        let mut result2 = Arc::clone(&action).commit(&table).await.unwrap();
        let updates2 = result2.take_updates();

        assert!(!updates1.is_empty());
        assert!(!updates2.is_empty());
    }

    /// Using both overwrite_by_row_filter and delete_file should fail validation.
    #[tokio::test]
    async fn test_overwrite_row_filter_and_delete_file_conflict() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        let f2 = make_data_file(&table, "test/2.parquet", 20, 200);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Predicate::AlwaysTrue)
            .delete_file(f1)
            .add_file(f2);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("Cannot use both overwrite_by_row_filter and delete_file"),
        );
    }

    /// overwrite_by_row_filter with no adds and no matches → still succeeds
    /// (no-op overwrite).
    #[tokio::test]
    async fn test_overwrite_by_row_filter_no_adds_no_matches() {
        use crate::expr::Reference;
        use crate::spec::Datum;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1]).await;

        // Filter x > 9999 → no files match, no adds → still produces a snapshot.
        let filter = Reference::new("x").greater_than(Datum::long(9999));
        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().overwrite_by_row_filter(filter);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
    }
}
