pub(crate) mod edit;

use std::{fs::File, sync::Arc};

use arrow::{
    array::{GenericBinaryArray, RecordBatch, Scalar},
    compute::kernels::cmp::eq,
};
use async_lock::RwLock;
use executor::fs;
use executor::futures::AsyncWriteExt;
use parquet::arrow::{
    arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter},
    ProjectionMask,
};
use snowflake::ProcessUniqueId;

use crate::{scope::Scope, serdes::Encode, DbOption, Offset};
use crate::serdes::Decode;
use crate::version::edit::VersionEdit;

pub(crate) type SyncVersion<K> = Arc<RwLock<Version<K>>>;

pub(crate) struct Version<K>
where
    K: Encode + Decode + Ord,
{
    // FIXME: only level 0
    pub(crate) level_slice: Vec<Scope<K>>,
    pub(crate) log: fs::File
}

impl<K> Version<K>
where
    K: Encode + Decode + Ord,
{
    pub(crate) async fn new(option: &DbOption) -> Result<Self, <K as Decode>::Error> {
        let mut log = fs::File::from(File::open(option.version_path()).unwrap());
        let mut level_slice = vec![];

        let edits = VersionEdit::recover(&mut log).await;

        let mut version = Version {
            level_slice,
            log,
        };
        apply_edits(&mut version, edits, option).await.unwrap();

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

pub(crate) async fn apply_edits<K: Encode + Decode + Ord>(version: &mut Version<K>, version_edits: Vec<VersionEdit<K>>, option: &DbOption) -> Result<(), <K as Encode>::Error> {
    for version_edit in version_edits {
        match version_edit {
            VersionEdit::Add { scope } => {
                scope.encode(&mut version.log).await?;
                version.level_slice.push(scope);
            }
            VersionEdit::Remove { .. } => {
                todo!()
            }
        }
    }
    Ok(())
}
