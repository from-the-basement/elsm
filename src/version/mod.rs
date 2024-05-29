use std::{fs::File, sync::Arc};

use arrow::{
    array::{GenericBinaryArray, RecordBatch, Scalar},
    compute::kernels::cmp::eq,
};
use async_lock::RwLock;
use parquet::arrow::{
    arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter},
    ProjectionMask,
};
use snowflake::ProcessUniqueId;

use crate::{scope::Scope, serdes::Encode, DbOption, Offset};

pub(crate) type SyncVersion<K> = Arc<RwLock<Version<K>>>;

#[derive(Debug)]
pub(crate) struct Version<K>
where
    K: Encode + Ord,
{
    // FIXME: only level 0
    pub(crate) level_slice: Vec<Scope<K>>,
}

impl<K> Version<K>
where
    K: Encode + Ord,
{
    pub(crate) async fn query(
        &self,
        key: &K,
        option: &DbOption,
    ) -> Result<Option<RecordBatch>, K::Error> {
        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes).await?;

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
    ) -> Result<Option<RecordBatch>, K::Error> {
        let file = File::open(option.table_path(scope_gen))?;

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
