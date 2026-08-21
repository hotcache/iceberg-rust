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

//! Transaction action for row-level deltas.
//!
//! [`RowDeltaAction`] adds data files and/or delete files (position or
//! equality deletes) to a table in a single atomic operation. This is the
//! primary mechanism for row-level updates and deletes in Iceberg.
//!
//! The resulting snapshot uses [`Operation::Overwrite`].

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{DataContentType, DataFile, Operation};
use crate::table::Table;
use crate::transaction::merging::MergingSnapshotProducer;
use crate::transaction::validate::validate_data_files_exist;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// A transaction action that applies row-level changes to a table.
///
/// This is the Rust equivalent of Java's `BaseRowDelta`. It uses
/// [`MergingSnapshotProducer`] to handle manifest creation for both
/// data files and delete files.
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.row_delta()
///     .add_rows(new_data_file)
///     .add_deletes(position_delete_file);
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
pub struct RowDeltaAction {
    producer: MergingSnapshotProducer,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            producer: MergingSnapshotProducer::new(Operation::Overwrite),
        }
    }

    /// Add a data file to the table.
    pub fn add_rows(mut self, file: DataFile) -> Self {
        self.producer.add_data_file(file);
        self
    }

    /// Add a delete file (position or equality) to the table.
    pub fn add_deletes(mut self, file: DataFile) -> Self {
        self.producer.add_delete_file(file);
        self
    }

    fn validate(&self) -> Result<()> {
        if !self.producer.has_added_data_files() && !self.producer.has_added_delete_files()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RowDelta requires at least one data file or delete file to add",
            ));
        }

        // Validate delete files have correct content type.
        for file in self.producer.added_delete_files() {
            if file.content_type() == DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add a data file as a delete file: {}",
                        file.file_path
                    ),
                ));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.validate()?;

        // Validate that data files referenced by position deletes still exist.
        let referenced_paths: Vec<String> = self
            .producer
            .added_delete_files()
            .iter()
            .filter_map(|f| f.referenced_data_file.clone())
            .collect();
        validate_data_files_exist(table, &referenced_paths).await?;

        self.producer.commit_snapshot(table).await
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::Operation;
    use crate::transaction::tests::{
        append_files, make_data_file, make_position_delete_file, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};

    /// Add data files and position delete files in a single RowDelta.
    #[tokio::test]
    async fn test_row_delta_with_data_and_deletes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append a data file first.
        let f1 = make_data_file(&table, "test/1.parquet", 100, 1000);
        let table = append_files(&catalog, &table, vec![f1]).await;

        // Apply a row delta: add a new data file + a position delete.
        let f2 = make_data_file(&table, "test/2.parquet", 50, 500);
        let del = make_position_delete_file(&table, "test/del-1.parquet", 5, "test/1.parquet");

        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_rows(f2).add_deletes(del);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        let summary = &snapshot.summary().additional_properties;
        // 2 data files total (f1 + f2).
        assert_eq!(summary.get("total-data-files").unwrap(), "2");
        // 1 delete file added.
        assert_eq!(summary.get("added-delete-files").unwrap(), "1");
        assert_eq!(summary.get("added-position-delete-files").unwrap(), "1");
    }

    /// Add only delete files (no new data).
    #[tokio::test]
    async fn test_row_delta_deletes_only() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 100, 1000);
        let table = append_files(&catalog, &table, vec![f1]).await;

        let del = make_position_delete_file(&table, "test/del-1.parquet", 5, "test/1.parquet");
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(del);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("added-delete-files")
                .unwrap(),
            "1"
        );
    }

    /// Empty row delta → error.
    #[tokio::test]
    async fn test_row_delta_empty() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let tx = Transaction::new(&table);
        let action = tx.row_delta();
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("at least one data file or delete file")
        );
    }

    /// Adding a data file as a delete should fail.
    #[tokio::test]
    async fn test_row_delta_rejects_data_file_as_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 100, 1000);
        let tx = Transaction::new(&table);
        // Intentionally pass a data file to add_deletes.
        let action = tx.row_delta().add_deletes(f1);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("Cannot add a data file as a delete file")
        );
    }

    /// Position delete referencing a non-existent data file should fail.
    #[tokio::test]
    async fn test_row_delta_rejects_delete_for_missing_data_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 100, 1000);
        let table = append_files(&catalog, &table, vec![f1]).await;

        // Position delete referencing a file that doesn't exist.
        let del = make_position_delete_file(&table, "test/del-1.parquet", 5, "test/ghost.parquet");
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(del);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("reference missing data files"),
            "expected missing data file error"
        );
    }
}
