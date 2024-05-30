pub(crate) mod edit;

use std::{
    fs::{File, OpenOptions},
    io::SeekFrom,
    sync::Arc,
};

use arrow::{
    array::{GenericBinaryArray, RecordBatch, Scalar},
    compute::kernels::cmp::eq,
};
use async_lock::RwLock;
use executor::{fs, futures::AsyncSeekExt};
use parquet::arrow::{
    arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter},
    ProjectionMask,
};
use snowflake::ProcessUniqueId;

use crate::{
    scope::Scope,
    serdes::{Decode, Encode},
    version::edit::VersionEdit,
    DbOption, Offset,
};

pub(crate) type SyncVersion<K> = Arc<RwLock<Version<K>>>;

pub(crate) struct Version<K>
where
    K: Encode + Decode + Ord,
{
    // FIXME: only level 0
    pub(crate) level_slice: Vec<Scope<K>>,
    pub(crate) log: fs::File,
}

impl<K> Version<K>
where
    K: Encode + Decode + Ord,
{
    pub(crate) async fn new(option: &DbOption) -> Result<Self, <K as Decode>::Error> {
        let mut log = fs::File::from(
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(option.version_path())
                .unwrap(),
        );
        let edits = VersionEdit::recover(&mut log).await;

        log.seek(SeekFrom::End(0)).await?;
        let mut version = Version {
            level_slice: vec![],
            log,
        };
        apply_edits(&mut version, edits, true).await.unwrap();

        Ok(version)
    }

    pub(crate) async fn query(
        &self,
        key: &K,
        option: &DbOption,
    ) -> Result<Option<RecordBatch>, <K as Decode>::Error> {
        let mut key_bytes = Vec::new();
        // FIXME: unwrap
        key.encode(&mut key_bytes).await.unwrap();

        let key_scalar = GenericBinaryArray::<Offset>::from(vec![key_bytes.as_slice()]);

        for scope in self.level_slice.iter().rev() {
            if !scope.is_between(key) {
                continue;
            }
            if let Some(batch) = Self::read_parquet(&scope.gen, &key_scalar, option).await? {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }

    async fn read_parquet(
        scope_gen: &ProcessUniqueId,
        key_scalar: &GenericBinaryArray<Offset>,
        option: &DbOption,
    ) -> Result<Option<RecordBatch>, <K as Decode>::Error> {
        let file = File::open(option.table_path(scope_gen))?;

        // FIXME: Async Reader
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(8192);
        let file_metadata = builder.metadata().file_metadata();

        let scalar = key_scalar.clone();
        let filter = ArrowPredicateFn::new(
            ProjectionMask::roots(file_metadata.schema_descr(), [0]),
            move |record_batch| eq(record_batch.column(0), &Scalar::new(&scalar)),
        );
        let row_filter = RowFilter::new(vec![Box::new(filter)]);
        builder = builder.with_row_filter(row_filter);

        let mut stream = builder.build().unwrap();

        if let Some(result) = stream.next() {
            return Ok(Some(result.unwrap()));
        }
        Ok(None)
    }
}

pub(crate) async fn apply_edits<K: Encode + Decode + Ord>(
    version: &mut Version<K>,
    version_edits: Vec<VersionEdit<K>>,
    is_recover: bool,
) -> Result<(), <K as Encode>::Error> {
    for version_edit in version_edits {
        if !is_recover {
            version_edit.encode(&mut version.log).await?;
        }
        match version_edit {
            VersionEdit::Add { scope } => {
                version.level_slice.push(scope);
            }
            VersionEdit::Remove { .. } => {
                todo!()
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{executor::block_on, io::Cursor};

    use crate::{scope::Scope, serdes::Encode, version::edit::VersionEdit};

    #[test]
    fn encode_and_decode() {
        block_on(async {
            let edits = vec![
                VersionEdit::Add {
                    scope: Scope {
                        min: Arc::new("Min".to_string()),
                        max: Arc::new("Max".to_string()),
                        gen: Default::default(),
                    },
                },
                VersionEdit::Remove {
                    gen: Default::default(),
                },
            ];

            let bytes = {
                let mut cursor = Cursor::new(vec![]);

                for edit in edits.clone() {
                    edit.encode(&mut cursor).await.unwrap();
                }
                cursor.into_inner()
            };

            let decode_edits = {
                let mut cursor = Cursor::new(bytes);

                VersionEdit::<String>::recover(&mut cursor).await
            };

            assert_eq!(edits, decode_edits);
        })
    }
}
