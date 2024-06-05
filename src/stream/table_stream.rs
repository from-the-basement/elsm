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
use executor::futures::Stream;
use parquet::arrow::{
    arrow_reader::{
        ArrowPredicate, ArrowPredicateFn, ParquetRecordBatchReader,
        ParquetRecordBatchReaderBuilder, RowFilter,
    },
    ProjectionMask,
};
use pin_project::pin_project;
use snowflake::ProcessUniqueId;

use crate::{
    serdes::{Decode, Encode},
    stream::batch_stream::BatchStream,
    DbOption, Offset,
};

#[pin_project]
pub(crate) struct TableStream<'stream, K, V>
where
    K: Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    inner: ParquetRecordBatchReader,
    stream: BatchStream<K, V>,
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
        lower: Option<K>,
        upper: Option<K>,
    ) -> Self {
        // FIXME: unwrap
        let file = File::open(option.table_path(gen)).unwrap();

        let lower = if let Some(l) = lower {
            Some(Self::to_scalar(l).await)
        } else {
            None
        };
        let upper = if let Some(u) = upper {
            Some(Self::to_scalar(u).await)
        } else {
            None
        };

        // FIXME: Async Reader
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(8192);
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

        // FIXME: unwrap
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        TableStream {
            inner: reader,
            stream: BatchStream::new(batch),
            _p: Default::default(),
        }
    }

    async fn to_scalar(key: K) -> GenericByteArray<GenericBinaryType<Offset>> {
        let mut key_bytes = Vec::new();
        key.encode(&mut key_bytes).await.unwrap();

        GenericBinaryArray::<Offset>::from(vec![key_bytes.as_slice()])
    }
}

impl<K, V> Stream for TableStream<'_, K, V>
where
    K: Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<V>), V::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(None) => {
                match self.inner.next() {
                    None => Poll::Ready(None),
                    Some(result) => {
                        // FIXME: unwrap
                        let batch = result.unwrap();

                        self.stream = BatchStream::new(batch);
                        self.poll_next(cx)
                    }
                }
            }
            poll => poll,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::path::PathBuf;
//     use executor::ExecutorBuilder;
//     use executor::futures::StreamExt;
//     use snowflake::ProcessUniqueId;
//     use crate::DbOption;
//     use crate::stream::table_stream::TableStream;
//
//     #[test]
//     fn iter() {
//         ExecutorBuilder::new().build().unwrap().block_on(async {
//             let option = DbOption {
//                 // TIPS: kv size in test case is 17
//                 path: PathBuf::from("E:/test"),
//                 max_mem_table_size: 25,
//                 immutable_chunk_num: 1,
//                 major_threshold_with_sst_size: 10,
//                 level_sst_magnification: 10,
//                 sst_file_size: 2 * 1024 * 1024,
//             };
//             let mut stream = TableStream::<String, String>::new(&option, &ProcessUniqueId::new(),
// None, None).await;             while let Some(result) = stream.next().await {
//                 println!("{}", result.unwrap().0)
//             }
//         })
//     }
// }
