use std::{
    collections::{btree_map::Entry, BTreeMap},
    hash::Hash,
    sync::Arc,
};

use thiserror::Error;

use crate::{oracle::WriteConflict, GetWrite};

#[derive(Debug)]
pub struct Transaction<K, V, DB>
where
    K: Ord,
    DB: GetWrite<K, V>,
{
    pub(crate) read_at: DB::Timestamp,
    pub(crate) local: BTreeMap<Arc<K>, Option<V>>,
    share: Arc<DB>,
}

impl<K, V, DB> Transaction<K, V, DB>
where
    K: Hash + Ord,
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
        self.share.read_commit(self.read_at.clone());
        if self.local.is_empty() {
            return Ok(());
        }
        let write_at = self.share.start_write();
        self.share.write_commit(
            self.read_at.clone(),
            write_at.clone(),
            self.local.keys().cloned().collect(),
        )?;
        self.share
            .write_batch(
                self.local
                    .into_iter()
                    .map(|(k, v)| (k, write_at.clone(), v)),
            )
            .await?;
        Ok(())
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
