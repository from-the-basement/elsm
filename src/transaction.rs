use std::{
    collections::{btree_map, btree_map::Entry, BTreeMap},
    fmt::Debug,
    marker::PhantomData,
    ops::Bound,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::Stream;
use pin_project::pin_project;
use thiserror::Error;

use crate::{
    oracle::{TimeStamp, WriteConflict},
    schema::Schema,
    stream::{merge_stream::MergeStream, EStreamImpl, StreamError},
    GetWrite,
};

#[derive(Debug)]
pub struct Transaction<S, DB>
where
    S: Schema,
    DB: GetWrite<S>,
{
    pub(crate) read_at: TimeStamp,
    pub(crate) local: BTreeMap<Arc<S::PrimaryKey>, Option<S>>,
    share: Arc<DB>,
}

impl<S, DB> Transaction<S, DB>
where
    S: Schema,
    DB: GetWrite<S>,
{
    pub(crate) fn new(share: Arc<DB>) -> Self {
        let read_at = share.start_read();
        Self {
            read_at,
            local: BTreeMap::new(),
            share,
        }
    }

    pub async fn get<G, F>(&self, key: &Arc<S::PrimaryKey>, f: F) -> Option<G>
    where
        F: Fn(&S) -> G + Sync + 'static,
        G: Send + 'static,
    {
        match self.local.get(key).and_then(|v| v.as_ref()) {
            Some(v) => Some((f)(v)),
            None => self.share.get(key, &self.read_at, f).await,
        }
    }

    pub fn set(&mut self, key: S::PrimaryKey, value: S) {
        self.entry(key, Some(value))
    }

    pub fn remove(&mut self, key: S::PrimaryKey) {
        self.entry(key, None)
    }

    fn entry(&mut self, key: S::PrimaryKey, value: Option<S>) {
        match self.local.entry(Arc::from(key)) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    pub async fn commit(self) -> Result<(), CommitError<S::PrimaryKey>> {
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
        lower: Option<&Arc<S::PrimaryKey>>,
        upper: Option<&Arc<S::PrimaryKey>>,
        f: F,
    ) -> Result<MergeStream<S, G, F>, StreamError<S::PrimaryKey, S>>
    where
        G: Send + Sync + 'static,
        F: Fn(&S) -> G + Send + Sync + 'static + Copy,
    {
        let mut iters = self
            .share
            .inner_range(lower, upper, &self.read_at, f)
            .await?;
        let range = self
            .local
            .range::<Arc<S::PrimaryKey>, (Bound<&Arc<S::PrimaryKey>>, Bound<&Arc<S::PrimaryKey>>)>(
                (
                    lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
                    upper.map(Bound::Included).unwrap_or(Bound::Unbounded),
                ),
            );
        let iter = TransactionStream {
            range,
            f,
            _p: Default::default(),
        };
        iters.insert(0, EStreamImpl::TransactionInner(iter));

        MergeStream::new(iters).await
    }
}

#[pin_project]
pub(crate) struct TransactionStream<'a, S, G, F, E>
where
    S: Schema,
    G: Send + Sync + 'static,
    F: Fn(&S) -> G + Sync + 'static,
{
    #[pin]
    range: btree_map::Range<'a, Arc<S::PrimaryKey>, Option<S>>,
    f: F,
    _p: PhantomData<E>,
}

impl<'a, S, E, G, F> Stream for TransactionStream<'a, S, G, F, E>
where
    S: Schema,
    G: Send + Sync + 'static,
    F: Fn(&S) -> G + Sync + 'static,
{
    type Item = Result<(Arc<S::PrimaryKey>, Option<G>), E>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        Poll::Ready(
            this.range
                .next()
                .map(|(key, value)| (key.clone(), value.as_ref().map(|v| (this.f)(v))))
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
