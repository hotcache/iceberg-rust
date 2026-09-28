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
//! logical contents of the table. Files to delete are named explicitly; the
//! resulting operation depends on whether files are added, deleted, or both.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{DataFile, Operation};
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
/// - **No conflict detection**: concurrent modifications between the time
///   files are read and committed are not detected.
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.overwrite_files()
///     .delete_file(old_file)
///     .add_file(new_file);
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
pub struct OverwriteFilesAction {
    /// The producer's `operation` field is not used directly — the
    /// operation type is determined dynamically at commit time based on
    /// which files were added/deleted.
    producer: MergingSnapshotProducer,
}

impl OverwriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            // The operation here is a placeholder; the actual operation is
            // determined dynamically by `self.operation()` at commit time.
            producer: MergingSnapshotProducer::new(Operation::Overwrite),
        }
    }

    /// Register a data file to be removed from the table.
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
        if !self.producer.has_added_data_files() && !self.producer.has_deleted_data_files() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Overwrite requires at least one file to add or delete",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for OverwriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.validate()?;

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

#[cfg(test)]
mod tests {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::Operation;
    use crate::transaction::tests::{
        append_files, make_data_file, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};

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
                .contains("Overwrite requires at least one file to add or delete")
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
}
