use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::Stream;
use pin_project::pin_project;
use thiserror::Error;

use crate::{
    index_batch::stream::IndexBatchStream,
    mem_table::stream::MemTableStream,
    serdes::{Decode, Encode},
    stream::{buf_stream::BufStream, level_stream::LevelStream, table_stream::TableStream},
    transaction::TransactionStream,
};

pub(crate) mod batch_stream;
pub(crate) mod buf_stream;
pub(crate) mod level_stream;
pub(crate) mod merge_inner_stream;
pub(crate) mod merge_stream;
pub(crate) mod table_stream;

#[pin_project(project = EStreamImplProj)]
pub(crate) enum EStreamImpl<'a, K, V, G, F>
where
    K: Ord + Encode + Decode,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    Buf(#[pin] BufStream<'a, K, G, StreamError<K, V>>),
    IndexBatch(#[pin] IndexBatchStream<'a, K, V, G, F>),
    MemTable(#[pin] MemTableStream<'a, K, V, G, F>),
    TransactionInner(#[pin] TransactionStream<'a, K, V, G, F, StreamError<K, V>>),
}

#[pin_project(project = EInnerStreamImplProj)]
pub(crate) enum EInnerStreamImpl<'a, K, V>
where
    K: Encode + Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    Table(#[pin] TableStream<'a, K, V>),
    Level(#[pin] LevelStream<'a, K, V>),
}

impl<'a, K, V, G, F> Stream for EStreamImpl<'a, K, V, G, F>
where
    K: Ord + Debug + Encode + Decode,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<G>), StreamError<K, V>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EStreamImplProj::Buf(stream) => stream.poll_next(cx),
            EStreamImplProj::IndexBatch(stream) => stream.poll_next(cx),
            EStreamImplProj::MemTable(stream) => stream.poll_next(cx),
            EStreamImplProj::TransactionInner(stream) => stream.poll_next(cx),
        }
    }
}

impl<'a, K, V> Stream for EInnerStreamImpl<'a, K, V>
where
    K: Ord + Encode + Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<V>), StreamError<K, V>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EInnerStreamImplProj::Table(stream) => stream.poll_next(cx),
            EInnerStreamImplProj::Level(stream) => stream.poll_next(cx),
        }
    }
}

#[derive(Debug, Error)]
pub enum StreamError<K, V>
where
    K: Encode + Decode,
    V: Decode,
{
    #[error("compaction key encode error: {0}")]
    KeyEncode(#[source] <K as Encode>::Error),
    #[error("compaction key decode error: {0}")]
    KeyDecode(#[source] <K as Decode>::Error),
    #[error("compaction value decode error: {0}")]
    ValueDecode(#[source] <V as Decode>::Error),
    #[error("compaction io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("compaction arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("compaction parquet error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
}
