use std::{
    collections::{btree_map, btree_map::Entry, BTreeMap},
    hash::Hash,
    io,
    marker::PhantomData,
    ops::Bound,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::Stream;
use thiserror::Error;

use crate::{
    iterator::merge_iterator::MergeIterator, oracle::WriteConflict, serdes::Decode, GetWrite,
};

#[derive(Debug)]
pub struct Transaction<K, V, DB>
where
    K: Ord,
    V: Decode,
    DB: GetWrite<K, V>,
{
    pub(crate) read_at: DB::Timestamp,
    pub(crate) local: BTreeMap<Arc<K>, Option<V>>,
    share: Arc<DB>,
}

impl<K, V, DB> Transaction<K, V, DB>
where
    K: Hash + Ord,
    V: Decode + Send + Sync,
    DB: GetWrite<K, V>,
    DB::Timestamp: Send + Sync,
{
    pub(crate) fn new(share: Arc<DB>) -> Self {
        let read_at = share.start_read();
        Self {
            read_at,
            local: BTreeMap::new(),
            share,
        }
    }

    pub async fn get<G, F>(&self, key: &Arc<K>, f: F) -> Option<G>
    where
        F: Fn(&V) -> G + Sync + 'static,
        G: Send + 'static,
    {
        match self.local.get(key).and_then(|v| v.as_ref()) {
            Some(v) => Some((f)(v)),
            None => self.share.get(key, &self.read_at, f).await,
        }
    }

    pub fn set(&mut self, key: K, value: V) {
        self.entry(key, Some(value))
    }

    pub fn remove(&mut self, key: K) {
        self.entry(key, None)
    }

    fn entry(&mut self, key: K, value: Option<V>) {
        match self.local.entry(Arc::from(key)) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    pub async fn commit(self) -> Result<(), CommitError<K>> {
        self.share.read_commit(self.read_at);
        if self.local.is_empty() {
            return Ok(());
        }
        let write_at = self.share.start_write();
        self.share
            .write_commit(self.read_at, write_at, self.local.keys().cloned().collect())?;
        self.share
            .write_batch(self.local.into_iter().map(|(k, v)| (k, write_at, v)))
            .await?;
        Ok(())
    }

    pub async fn range<G, F>(
        &self,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
        f: F,
    ) -> Result<MergeIterator<K, V, G>, V::Error>
    where
        G: Send + Sync + 'static,
        F: Fn(&V) -> G + Send + Sync + 'static + Copy,
    {
        let mut iters = self
            .share
            .inner_range(lower, upper, &self.read_at, f)
            .await?;
        let range = self
            .local
            .range::<Arc<K>, (Bound<&Arc<K>>, Bound<&Arc<K>>)>((
                lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
                upper.map(Bound::Included).unwrap_or(Bound::Unbounded),
            ));
        let iter = TransactionIter {
            range,
            f,
            _p: Default::default(),
        };
        iters.insert(0, Box::pin(iter));

        MergeIterator::new(iters).await
    }
}

pub(crate) struct TransactionIter<'a, K, V, G, F, E>
where
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    range: btree_map::Range<'a, Arc<K>, Option<V>>,
    f: F,
    _p: PhantomData<E>,
}

impl<'a, K, V, E, G, F> Stream for TransactionIter<'a, K, V, G, F, E>
where
    K: Ord,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
    E: From<io::Error> + std::error::Error + Send + Sync + 'static,
{
    type Item = Result<(&'a Arc<K>, Option<G>), E>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(
            self.range
                .next()
                .map(|(key, value)| (key, value.as_ref().map(|v| (self.f)(v))))
                .map(Ok),
        )
    }
}

#[derive(Debug, Error)]
pub enum CommitError<K> {
    WriteConflict(Vec<Arc<K>>),
    WriteError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<K> From<WriteConflict<K>> for CommitError<K> {
    fn from(e: WriteConflict<K>) -> Self {
        CommitError::WriteConflict(e.to_keys())
    }
}
