use std::hash::Hash;
use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use thiserror::Error;

use crate::oracle::Oracle;
use crate::GetWrite;

#[derive(Debug)]
pub struct Transaction<K, V, DB>
where
    K: Ord,
    DB: Oracle<K> + GetWrite<K, V>,
{
    pub(crate) read_at: DB::Timestamp,
    pub(crate) write_at: Option<DB::Timestamp>,
    pub(crate) local: BTreeMap<Arc<K>, Option<V>>,
    share: Arc<DB>,
}

impl<K, V, DB> Transaction<K, V, DB>
where
    K: Hash + Ord + Send + 'static,
    V: Sync + Send + 'static,
    DB: Oracle<K> + GetWrite<K, V>,
    DB::Timestamp: Send + 'static,
{
    pub(crate) async fn new(share: Arc<DB>) -> Self {
        let read_at = share.read();
        Self {
            read_at,
            write_at: None,
            local: BTreeMap::new(),
            share,
        }
    }

    pub async fn get<G, F, Q>(&self, key: &Q, f: F) -> Option<G>
    where
        Q: ?Sized + Ord,
        F: FnOnce(&V) -> G,
        Arc<K>: Borrow<Q>,
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

    pub(crate) fn entry(&mut self, key: K, value: Option<V>) {
        match self.local.entry(Arc::from(key)) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    pub async fn commit(mut self) -> Result<(), CommitError<K>> {
        self.share.read_commit(self.read_at.clone());
        if !self.local.is_empty() {
            let write_at = self.share.tick();
            self.write_at = Some(write_at.clone());
            self.share
                .write_commit(
                    self.read_at.clone(),
                    write_at.clone(),
                    self.local.keys().cloned().collect(),
                )
                .map_err(|e| CommitError::WriteConflict(e.to_keys()))?;
            self.share
                .write_batch(
                    self.local
                        .into_iter()
                        .map(|(k, v)| (k, write_at.clone(), v)),
                )
                .await
                .map_err(|e| CommitError::WriteError(e))?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CommitError<K> {
    WriteConflict(Vec<Arc<K>>),
    WriteError(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}
