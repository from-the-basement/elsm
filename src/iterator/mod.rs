use std::sync::Arc;

use crate::{
    index_batch::IndexBatchIterator, iterator::buf_iterator::BufIterator,
    mem_table::MemTableIterator, serdes::Decode, transaction::TransactionIter, EIterator,
};

pub(crate) mod buf_iterator;
pub(crate) mod merge_iterator;

pub(crate) enum EIteratorImpl<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    Buf(BufIterator<'a, K, G, V::Error>),
    IndexBatch(IndexBatchIterator<'a, K, T, V, G, F>),
    MemTable(MemTableIterator<'a, K, V, T, G, F>),
    TransactionInner(TransactionIter<'a, K, V, G, F, V::Error>),
}

impl<'a, K, T, V, G, F> EIteratorImpl<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    pub(crate) async fn try_next(&mut self) -> Result<Option<(&'a Arc<K>, Option<G>)>, V::Error> {
        match self {
            EIteratorImpl::Buf(iter) => iter.try_next().await,
            EIteratorImpl::IndexBatch(iter) => iter.try_next().await,
            EIteratorImpl::MemTable(iter) => iter.try_next().await,
            EIteratorImpl::TransactionInner(iter) => iter.try_next().await,
        }
    }
}
