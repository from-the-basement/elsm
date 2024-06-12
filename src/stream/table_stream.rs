use std::{
    fs::File,
    marker::PhantomData,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{GenericBinaryArray, GenericByteArray, Scalar},
    compute::kernels::cmp::{gt_eq, lt_eq},
    datatypes::GenericBinaryType,
};
use executor::{
    fs,
    futures::{Stream, StreamExt},
};
use parquet::arrow::{
    arrow_reader::{ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, RowFilter},
    async_reader::ParquetRecordBatchStream,
    ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use pin_project::pin_project;
use snowflake::ProcessUniqueId;

use crate::{
    serdes::{Decode, Encode},
    stream::{batch_stream::BatchStream, StreamError},
    DbOption, Offset,
};

#[pin_project]
pub(crate) struct TableStream<'stream, K, V>
where
    K: Encode + Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    inner: ParquetRecordBatchStream<fs::File>,
    stream: Option<BatchStream<K, V>>,
    _p: PhantomData<&'stream ()>,
}

impl<K, V> TableStream<'_, K, V>
where
    K: Encode + Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    pub(crate) async fn new(
        option: &DbOption,
        gen: &ProcessUniqueId,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
    ) -> Result<Self, StreamError<K, V>> {
        let lower = if let Some(l) = lower {
            Some(Self::to_scalar(l).await?)
        } else {
            None
        };
        let upper = if let Some(u) = upper {
            Some(Self::to_scalar(u).await?)
        } else {
            None
        };

        let mut file = fs::File::from(File::open(option.table_path(gen)).map_err(StreamError::Io)?);
        let meta = ArrowReaderMetadata::load_async(&mut file, Default::default())
            .await
            .map_err(StreamError::Parquet)?;
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, meta);
        let file_metadata = builder.metadata().file_metadata();

        let mut predicates = Vec::with_capacity(2);

        if let Some(lower_scalar) = lower {
            predicates.push(Box::new(ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [0]),
                move |record_batch| gt_eq(record_batch.column(0), &Scalar::new(&lower_scalar)),
            )) as Box<dyn ArrowPredicate>)
        }
        if let Some(upper_scalar) = upper {
            predicates.push(Box::new(ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [0]),
                move |record_batch| lt_eq(record_batch.column(0), &Scalar::new(&upper_scalar)),
            )) as Box<dyn ArrowPredicate>)
        }

        let row_filter = RowFilter::new(predicates);
        builder = builder.with_row_filter(row_filter);

        let mut reader = builder.build().map_err(StreamError::Parquet)?;

        let mut stream = None;
        if let Some(result) = reader.next().await {
            stream = Some(BatchStream::new(result.map_err(StreamError::Parquet)?));
        }

        Ok(TableStream {
            inner: reader,
            stream,
            _p: Default::default(),
        })
    }

    async fn to_scalar(
        key: &K,
    ) -> Result<GenericByteArray<GenericBinaryType<Offset>>, StreamError<K, V>> {
        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes)
            .await
            .map_err(StreamError::KeyEncode)?;

        Ok(GenericBinaryArray::<Offset>::from(vec![
            key_bytes.as_slice()
        ]))
    }
}

impl<K, V> Stream for TableStream<'_, K, V>
where
    K: Encode + Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<V>), StreamError<K, V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stream.is_none() {
            return Poll::Ready(None);
        }
        match Pin::new(self.stream.as_mut().unwrap()).poll_next(cx) {
            Poll::Ready(None) => match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.stream = Some(BatchStream::new(batch));
                    self.poll_next(cx)
                }
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(StreamError::Parquet(err)))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            poll => poll,
        }
    }
}
