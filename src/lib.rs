pub(crate) mod mem_table;
pub(crate) mod oracle;
pub mod record;
pub mod serdes;
pub mod transaction;
pub mod wal;

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::hash::Hash;
use std::mem;
use std::sync::atomic::AtomicU64;
use std::sync::RwLock;
use std::{borrow::Borrow, sync::Arc};

use mem_table::MemTable;
use oracle::Oracle;
use record::{Record, RecordType};
use transaction::{CommitError, Transaction};
use wal::{WalProvider, WalWrite};

#[derive(Debug)]
struct InMemDB<K, V, T>
where
    K: Ord,
    T: Ord,
{
    pub(crate) mutable: MemTable<K, V, T>,
    pub(crate) immutable: VecDeque<MemTable<K, V, T>>,
}

#[derive(Debug)]
pub struct DB<K, V, W, O, WP>
where
    K: Ord,
    O: Oracle<K>,
{
    in_mem: RwLock<InMemDB<K, V, O::Timestamp>>,
    wal: W,
    fid: AtomicU64,
    wal_provider: WP,
    pub(crate) oracle: O,
}

impl<K, V, W, O, WP> Oracle<K> for DB<K, V, W, O, WP>
where
    K: Ord,
    O: Oracle<K>,
{
    type Timestamp = O::Timestamp;

    fn read(&self) -> Self::Timestamp {
        self.oracle.read()
    }

    fn read_commit(&self, ts: Self::Timestamp) {
        self.oracle.read_commit(ts)
    }

    fn tick(&self) -> Self::Timestamp {
        self.oracle.tick()
    }

    fn write_commit(
        &self,
        read_at: Self::Timestamp,
        write_at: Self::Timestamp,
        in_write: HashSet<Arc<K>>,
    ) -> Result<(), oracle::WriteConflict<K>> {
        self.oracle.write_commit(read_at, write_at, in_write)
    }
}

pub trait GetWrite<K, V>: Oracle<K>
where
    K: Ord,
{
    fn get<G, Q>(
        &self,
        key: &Q,
        ts: &Self::Timestamp,
        f: impl FnOnce(&V) -> G,
    ) -> impl Future<Output = Option<G>>
    where
        Q: ?Sized + Ord,
        Arc<K>: Borrow<Q>;

    fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<K>, Self::Timestamp, Option<V>)>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>;
}

impl<K, V, W, O, WP> GetWrite<K, V> for DB<K, V, W, O, WP>
where
    K: Hash + Ord + Send + 'static,
    O: Oracle<K>,
    O::Timestamp: Send + 'static,
    W: WalWrite<Arc<K>, V, O::Timestamp>,
    W::Error: Send + Sync,
    K: 'static,
    V: Sync + Send + 'static,
{
    async fn get<G, Q>(&self, key: &Q, ts: &Self::Timestamp, f: impl FnOnce(&V) -> G) -> Option<G>
    where
        Q: ?Sized + Ord,
        Arc<K>: Borrow<Q>,
    {
        self.in_mem.read().unwrap().mutable.get(key, ts, f)
    }

    async fn write_batch(
        &self,
        mut kvs: impl ExactSizeIterator<Item = (Arc<K>, Self::Timestamp, Option<V>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        match kvs.len() {
            0 => Ok(()),
            1 => {
                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::Full, key, ts, value)
                    .await
                    .map_err(|e| e.into())
            }
            len => {
                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::First, key, ts, value).await?;

                for (key, ts, value) in (&mut kvs).take(len - 2) {
                    self.write(RecordType::First, key, ts, value).await?;
                }

                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::First, key, ts, value)
                    .await
                    .map_err(|e| e.into())
            }
        }
    }
}

impl<K, V, W, O, WP> DB<K, V, W, O, WP>
where
    K: Hash + Ord + Send + 'static,
    O: Oracle<K>,
    O::Timestamp: Send + 'static,
    W: WalWrite<Arc<K>, V, O::Timestamp>,
    W::Error: Send + Sync,
    V: Sync + Send + 'static,
{
    async fn write(
        &self,
        record_type: RecordType,
        key: Arc<K>,
        ts: O::Timestamp,
        value: Option<V>,
    ) -> Result<(), W::Error> {
        self.wal
            .write(Record::new(record_type, &key, &ts, value.as_ref()))
            .await?;
        self.in_mem.read().unwrap().mutable.insert(key, ts, value);
        Ok(())
    }

    fn freeze(&self)
    where
        WP: WalProvider,
    {
        let mut in_mem = self.in_mem.write().unwrap();
        let mut mutable = MemTable::default();
        mem::swap(&mut mutable, &mut in_mem.mutable);
        in_mem.immutable.push_front(mutable);

        self.fid.;
        let wal = self.wal_provider.open(self.fid);
    }

    pub async fn get<G, Q>(&self, k: &Q, f: impl FnOnce(&V) -> G) -> Option<G>
    where
        Q: ?Sized + Ord,
        Arc<K>: Borrow<Q>,
    {
        <Self as GetWrite<K, V>>::get(self, k, &self.oracle.read(), f).await
    }

    pub async fn write_batch(
        self: &Arc<Self>,
        kvs: impl ExactSizeIterator<Item = (K, Option<V>)>,
    ) -> Result<(), CommitError<K>> {
        let mut txn = self.new_txn().await;
        for (k, v) in kvs {
            txn.entry(k, v);
        }
        txn.commit().await
    }

    pub async fn new_txn(self: &Arc<Self>) -> Transaction<K, V, Self> {
        Transaction::new(self.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, RwLock};

    use futures::{executor::block_on, io::Cursor};

    use crate::InMemDB;
    use crate::{
        mem_table::MemTable, oracle::LocalOracle, transaction::CommitError, wal::WalFile, DB,
    };

    #[test]
    fn read_committed() {
        let file = Vec::new();
        let wal = WalFile::new(Cursor::new(file), crc32fast::Hasher::new);

        block_on(async {
            let lsm_tree = Arc::new(DB {
                in_mem: RwLock::new(InMemDB {
                    mutable: MemTable::new(&wal).await.unwrap(),
                    immutable: VecDeque::new(),
                }),
                wal,
                oracle: LocalOracle::default(),
                wal_provider: todo!(),
                fid: todo!(),
            });

            lsm_tree
                .write_batch(
                    [("key0".to_string(), Some(0)), ("key1".to_string(), Some(1))].into_iter(),
                )
                .await
                .unwrap();

            let mut t0 = lsm_tree.new_txn().await;
            let mut t1 = lsm_tree.new_txn().await;

            t0.set(
                "key0".into(),
                t0.get(&"key1".to_owned(), |v| *v).await.unwrap(),
            );
            t1.set(
                "key1".into(),
                t1.get(&"key0".to_owned(), |v| *v).await.unwrap(),
            );

            t0.commit().await.unwrap();
            t1.commit().await.unwrap();

            assert_eq!(
                lsm_tree.get(&Arc::from("key0".to_string()), |v| *v).await,
                Some(1)
            );
            assert_eq!(
                lsm_tree.get(&Arc::from("key1".to_string()), |v| *v).await,
                Some(0)
            );
        });
    }

    #[test]
    fn write_conflicts() {
        let file = Vec::new();
        let wal = WalFile::new(Cursor::new(file), crc32fast::Hasher::new);

        block_on(async {
            let lsm_tree = Arc::new(DB {
                in_mem: RwLock::new(InMemDB {
                    mutable: MemTable::new(&wal).await.unwrap(),
                    immutable: VecDeque::new(),
                }),
                wal,
                oracle: LocalOracle::default(),
                wal_provider: todo!(),
                fid: todo!(),
            });

            lsm_tree
                .write_batch(
                    [("key0".to_string(), Some(0)), ("key1".to_string(), Some(1))].into_iter(),
                )
                .await
                .unwrap();

            let mut t0 = lsm_tree.new_txn().await;
            let mut t1 = lsm_tree.new_txn().await;

            t0.set(
                "key0".into(),
                t0.get(&"key1".to_owned(), |v| *v).await.unwrap(),
            );
            t1.set(
                "key0".into(),
                t1.get(&"key0".to_owned(), |v| *v).await.unwrap(),
            );

            t0.commit().await.unwrap();

            let commit = t1.commit().await;
            assert!(commit.is_err());
            if let Err(CommitError::WriteConflict(keys)) = commit {
                assert_eq!(lsm_tree.get(&keys[0], |v| *v).await, Some(1));
                return;
            }
            unreachable!();
        });
    }
}
