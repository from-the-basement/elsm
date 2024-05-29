use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::Stream;
use pin_project::pin_project;

use crate::{
    index_batch::stream::IndexBatchStream, mem_table::stream::MemTableStream, serdes::Decode,
    stream::buf_stream::BufStream, transaction::TransactionStream,
};

pub(crate) mod buf_stream;
pub(crate) mod merge_stream;

#[pin_project(project = EStreamImplProj)]
pub(crate) enum EStreamImpl<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    Buf(#[pin] BufStream<'a, K, G, V::Error>),
    IndexBatch(#[pin] IndexBatchStream<'a, K, T, V, G, F>),
    MemTable(#[pin] MemTableStream<'a, K, T, V, G, F>),
    TransactionInner(#[pin] TransactionStream<'a, K, V, G, F, V::Error>),
}

impl<'a, K, T, V, G, F> Stream for EStreamImpl<'a, K, T, V, G, F>
where
    K: Ord + Debug,
    T: Ord + Copy + Default,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<G>), V::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EStreamImplProj::Buf(stream) => stream.poll_next(cx),
            EStreamImplProj::IndexBatch(stream) => stream.poll_next(cx),
            EStreamImplProj::MemTable(stream) => stream.poll_next(cx),
            EStreamImplProj::TransactionInner(stream) => stream.poll_next(cx),
        }
    }
}
