mod consistent_hash;
pub(crate) mod mem_table;
pub(crate) mod oracle;
pub(crate) mod record;
pub mod serdes;
pub mod transaction;
pub mod wal;

use std::{
    borrow::Borrow,
    collections::VecDeque,
    error,
    future::Future,
    hash::Hash,
    mem,
    sync::{Arc, RwLock},
};

use consistent_hash::jump_consistent_hash;
use executor::shard::Shard;
use mem_table::MemTable;
use oracle::Oracle;
use record::{Record, RecordType};
use transaction::Transaction;
use wal::WalWrite;

#[derive(Debug)]
struct LocalDB<K, V, T, W>
where
    K: Ord,
    T: Ord,
{
    mutable: MemTable<K, V, T>,
    wal: W,
}

#[derive(Debug)]
pub struct DB<K, V, W, O>
where
    K: Ord,
    O: Oracle<K>,
{
    pub(crate) oracle: O,
    pub(crate) mutable_shards: Shard<LocalDB<K, V, O::Timestamp, W>>,
    pub(crate) immutable: RwLock<VecDeque<MemTable<K, V, O::Timestamp>>>,
}

impl<K, V, W, O> DB<K, V, W, O>
where
    K: Ord + Hash + Send + Sync + 'static,
    V: Send + 'static,
    O: Oracle<K>,
    O::Timestamp: Send + Sync + 'static,
    W: WalWrite<Arc<K>, V, O::Timestamp> + 'static,
    W::Error: Send + Sync + 'static,
{
    pub fn new_txn(self: &Arc<Self>) -> Transaction<K, V, Self> {
        Transaction::new(self.clone())
    }

    async fn write(
        &self,
        record_type: RecordType,
        key: Arc<K>,
        ts: O::Timestamp,
        value: Option<V>,
    ) -> Result<(), W::Error> {
        let consistent_hash =
            jump_consistent_hash(fxhash::hash64(&key), executor::worker_num()) as usize;
        self.mutable_shards
            .with(consistent_hash, move |local| async move {
                let mut local = local.write().await;
                local
                    .wal
                    .write(Record::new(record_type, &key, &ts, value.as_ref()))
                    .await?;
                local.mutable.insert(key, ts, value);
                Ok::<_, W::Error>(())
            })
            .await
    }

    async fn get<G, Q>(
        &self,
        key: &Q,
        ts: &O::Timestamp,
        f: impl FnOnce(&V) -> G + Send + 'static,
    ) -> Option<G>
    where
        Q: ?Sized + Ord + Hash + Sync + 'static,
        Arc<K>: Borrow<Q>,
        G: Send + 'static,
        O::Timestamp: Sync,
    {
        let consistent_hash =
            jump_consistent_hash(fxhash::hash64(key), executor::worker_num()) as usize;

        // Safety: `key` and `ts` are guaranteed to be valid for the lifetime of the closure.
        let (key, ts) = unsafe {
            (
                mem::transmute::<_, &Q>(key),
                mem::transmute::<_, &O::Timestamp>(ts),
            )
        };

        self.mutable_shards
            .with(consistent_hash, move |local| async move {
                local.read().await.mutable.get(key, ts).map(f)
            })
            .await
    }

    async fn write_batch(
        &self,
        mut kvs: impl ExactSizeIterator<Item = (Arc<K>, O::Timestamp, Option<V>)>,
    ) -> Result<(), W::Error> {
        match kvs.len() {
            0 => Ok(()),
            1 => {
                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::Full, key, ts, value).await
            }
            len => {
                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::First, key, ts, value).await?;

                for (key, ts, value) in (&mut kvs).take(len - 2) {
                    self.write(RecordType::Middle, key, ts, value).await?;
                }

                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::Last, key, ts, value).await
            }
        }
    }
}

impl<K, V, W, O> Oracle<K> for DB<K, V, W, O>
where
    K: Ord,
    O: Oracle<K>,
{
    type Timestamp = O::Timestamp;

    fn start_read(&self) -> Self::Timestamp {
        self.oracle.start_read()
    }

    fn read_commit(&self, ts: Self::Timestamp) {
        self.oracle.read_commit(ts)
    }

    fn start_write(&self) -> Self::Timestamp {
        self.oracle.start_write()
    }

    fn write_commit(
        &self,
        read_at: Self::Timestamp,
        write_at: Self::Timestamp,
        in_write: std::collections::HashSet<Arc<K>>,
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
        f: impl FnOnce(&V) -> G + Send + 'static,
    ) -> impl Future<Output = Option<G>>
    where
        Q: ?Sized + Ord + Hash + Sync + 'static,
        Arc<K>: Borrow<Q>,
        G: Send + 'static,
        Self::Timestamp: Sync;

    fn write(
        &self,
        record_type: RecordType,
        key: Arc<K>,
        ts: Self::Timestamp,
        value: Option<V>,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync + 'static>>>;

    fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<K>, Self::Timestamp, Option<V>)>,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync + 'static>>>;
}

impl<K, V, W, O> GetWrite<K, V> for DB<K, V, W, O>
where
    K: Ord + Hash + Send + Sync + 'static,
    V: Send + 'static,
    O: Oracle<K>,
    O::Timestamp: Send + Sync + 'static,
    W: WalWrite<Arc<K>, V, O::Timestamp> + 'static,
    W::Error: Send + Sync + 'static,
{
    async fn write(
        &self,
        record_type: RecordType,
        key: Arc<K>,
        ts: O::Timestamp,
        value: Option<V>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        DB::write(self, record_type, key, ts, value).await?;
        Ok(())
    }

    async fn get<G, Q>(
        &self,
        key: &Q,
        ts: &O::Timestamp,
        f: impl FnOnce(&V) -> G + Send + 'static,
    ) -> Option<G>
    where
        Q: ?Sized + Ord + Hash + Sync + 'static,
        Arc<K>: Borrow<Q>,
        G: Send + 'static,
        O::Timestamp: Sync,
    {
        DB::get(self, key, ts, f).await
    }

    async fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<K>, O::Timestamp, Option<V>)>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        DB::write_batch(self, kvs).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, RwLock},
    };

    use executor::{shard::Shard, ExecutorBuilder};
    use futures::io::Cursor;

    use crate::{
        mem_table::MemTable, oracle::LocalOracle, transaction::CommitError, wal::WalFile, DB,
    };

    #[test]
    fn read_committed() {
        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(DB {
                mutable_shards: Shard::new(|| crate::LocalDB {
                    mutable: MemTable::default(),
                    wal: WalFile::new(Cursor::new(Vec::new()), crc32fast::Hasher::new),
                }),
                oracle: LocalOracle::default(),
                immutable: RwLock::new(VecDeque::default()),
            });

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), 0);
            txn.set("key1".to_string(), 1);
            txn.commit().await.unwrap();

            let mut t0 = db.new_txn();
            let mut t1 = db.new_txn();

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

            let txn = db.new_txn();

            assert_eq!(
                txn.get(&Arc::from("key0".to_string()), |v| *v).await,
                Some(1)
            );
            assert_eq!(
                txn.get(&Arc::from("key1".to_string()), |v| *v).await,
                Some(0)
            );
        });
    }

    #[test]
    fn write_conflicts() {
        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(DB {
                mutable_shards: Shard::new(|| crate::LocalDB {
                    mutable: MemTable::default(),
                    wal: WalFile::new(Cursor::new(Vec::new()), crc32fast::Hasher::new),
                }),
                oracle: LocalOracle::default(),
                immutable: RwLock::new(VecDeque::default()),
            });

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), 0);
            txn.set("key1".to_string(), 1);
            txn.commit().await.unwrap();

            let mut t0 = db.new_txn();
            let mut t1 = db.new_txn();

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
                assert_eq!(db.new_txn().get(&keys[0], |v| *v).await, Some(1));
                return;
            }
            panic!("unreachable");
        });
    }
}
