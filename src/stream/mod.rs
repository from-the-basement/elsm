use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::Stream;

use crate::{
    index_batch::stream::IndexBatchStream, mem_table::stream::MemTableStream, serdes::Decode,
    stream::buf_stream::BufStream, transaction::TransactionStream,
};

pub(crate) mod buf_stream;
pub(crate) mod merge_stream;

pub(crate) enum EStreamImpl<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    Buf(BufStream<'a, K, G, V::Error>),
    IndexBatch(IndexBatchStream<'a, K, T, V, G, F>),
    MemTable(MemTableStream<'a, K, V, T, G, F>),
    TransactionInner(TransactionStream<'a, K, V, G, F, V::Error>),
}

impl<'a, K, T, V, G, F> Stream for EStreamImpl<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<G>), V::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match unsafe { Pin::into_inner_unchecked(self) } {
            EStreamImpl::Buf(stream) => unsafe { Pin::new_unchecked(stream) }.poll_next(cx),
            EStreamImpl::IndexBatch(stream) => unsafe { Pin::new_unchecked(stream) }.poll_next(cx),
            EStreamImpl::MemTable(stream) => unsafe { Pin::new_unchecked(stream) }.poll_next(cx),
            EStreamImpl::TransactionInner(stream) => {
                unsafe { Pin::new_unchecked(stream) }.poll_next(cx)
            }
        }
    }
}
