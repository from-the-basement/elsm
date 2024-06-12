pub(crate) mod edit;
pub(crate) mod set;

use std::{fs::File, sync::Arc};

use arrow::{
    array::{GenericBinaryArray, RecordBatch, Scalar},
    compute::kernels::cmp::eq,
};
use executor::{fs, futures::StreamExt};
use parquet::arrow::{
    arrow_reader::{ArrowPredicateFn, ArrowReaderMetadata, RowFilter},
    ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use snowflake::ProcessUniqueId;
use thiserror::Error;

use crate::{
    scope::Scope,
    serdes::{Decode, Encode},
    DbOption, Offset,
};

pub const MAX_LEVEL: usize = 7;

pub(crate) type VersionRef<K> = Arc<Version<K>>;

pub(crate) struct Version<K>
where
    K: Encode + Decode + Ord,
{
    pub(crate) num: usize,
    pub(crate) level_slice: [Vec<Scope<K>>; MAX_LEVEL],
}

impl<K> Clone for Version<K>
where
    K: Encode + Decode + Ord,
{
    fn clone(&self) -> Self {
        let mut level_slice = Version::level_slice_new();

        for (level, scopes) in self.level_slice.iter().enumerate() {
            for scope in scopes {
                level_slice[level].push(scope.clone());
            }
        }

        Self {
            num: self.num,
            level_slice,
        }
    }
}

impl<K> Version<K>
where
    K: Encode + Decode + Ord,
{
    pub(crate) async fn query(
        &self,
        key: &K,
        option: &DbOption,
    ) -> Result<Option<RecordBatch>, VersionError<K>> {
        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes)
            .await
            .map_err(VersionError::<K>::Encode)?;

        let key_scalar = GenericBinaryArray::<Offset>::from(vec![key_bytes.as_slice()]);

        for scope in self.level_slice[0].iter().rev() {
            if !scope.is_between(key) {
                continue;
            }
            if let Some(batch) = Self::read_parquet(&scope.gen, &key_scalar, option).await? {
                return Ok(Some(batch));
            }
        }
        for level in self.level_slice[1..6].iter() {
            if level.is_empty() {
                continue;
            }
            let index = Self::scope_search(key, level);
            if !level[index].is_between(key) {
                continue;
            }
            if let Some(batch) = Self::read_parquet(&level[index].gen, &key_scalar, option).await? {
                return Ok(Some(batch));
            }
        }

        Ok(None)
    }

    pub(crate) fn scope_search(key: &K, level: &[Scope<K>]) -> usize {
        level
            .binary_search_by(|scope| scope.min.as_ref().cmp(key))
            .unwrap_or_else(|index| index.saturating_sub(1))
    }

    pub(crate) fn tables_len(&self, level: usize) -> usize {
        self.level_slice[level].len()
    }

    pub(crate) fn level_slice_new() -> [Vec<Scope<K>>; 7] {
        [
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ]
    }

    async fn read_parquet(
        scope_gen: &ProcessUniqueId,
        key_scalar: &GenericBinaryArray<Offset>,
        option: &DbOption,
    ) -> Result<Option<RecordBatch>, VersionError<K>> {
        let mut file =
            fs::File::from(File::open(option.table_path(scope_gen)).map_err(VersionError::Io)?);
        let meta = ArrowReaderMetadata::load_async(&mut file, Default::default())
            .await
            .map_err(VersionError::Parquet)?;
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, meta);
        let file_metadata = builder.metadata().file_metadata();

        let scalar = key_scalar.clone();
        let filter = ArrowPredicateFn::new(
            ProjectionMask::roots(file_metadata.schema_descr(), [0]),
            move |record_batch| eq(record_batch.column(0), &Scalar::new(&scalar)),
        );
        let row_filter = RowFilter::new(vec![Box::new(filter)]);
        builder = builder.with_row_filter(row_filter);

        let mut stream = builder.build().map_err(VersionError::Parquet)?;

        if let Some(result) = stream.next().await {
            return Ok(Some(result.map_err(VersionError::Parquet)?));
        }
        Ok(None)
    }
}

#[derive(Debug, Error)]
pub enum VersionError<K>
where
    K: Encode,
{
    #[error("version encode error: {0}")]
    Encode(#[source] <K as Encode>::Error),
    #[error("version io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("version arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("version parquet error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
}
