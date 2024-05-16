mod consistent_hash;
mod index_batch;
pub(crate) mod mem_table;
pub(crate) mod oracle;
pub(crate) mod record;
pub mod serdes;
pub mod transaction;
pub mod wal;

use std::{
    collections::{BTreeMap, VecDeque},
    error,
    fmt::Debug,
    future::Future,
    hash::Hash,
    io, mem,
    sync::Arc,
};

use arrow::{
    array::{GenericBinaryBuilder, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_lock::RwLock;
use consistent_hash::jump_consistent_hash;
use executor::shard::Shard;
use futures::{executor::block_on, io::Cursor, AsyncWrite};
use lazy_static::lazy_static;
use mem_table::MemTable;
use oracle::Oracle;
use record::{Record, RecordType};
use serdes::Encode;
use transaction::Transaction;
use wal::{provider::WalProvider, WalFile, WalManager, WalWrite, WriteError};

use crate::{index_batch::IndexBatch, serdes::Decode};

lazy_static! {
    pub static ref ELSM_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::LargeBinary, false),
            Field::new("value", DataType::LargeBinary, true),
        ]))
    };
}

pub type Offset = i64;

#[derive(Debug)]
pub struct DbOption {
    pub max_wal_size: usize,
    pub immutable_chunk_num: usize,
}

#[derive(Debug)]
struct MutableShard<K, V, T, W>
where
    K: Ord,
    T: Ord,
{
    mutable: MemTable<K, V, T>,
    wal: WalFile<W, Arc<K>, V, T>,
}

#[derive(Debug)]
pub struct Db<K, V, O, WP>
where
    K: Ord,
    O: Oracle<K>,
    WP: WalProvider,
{
    option: DbOption,
    pub(crate) oracle: O,
    wal_manager: Arc<WalManager<WP>>,
    pub(crate) mutable_shards:
        Shard<unsend::lock::RwLock<MutableShard<K, V, O::Timestamp, WP::File>>>,
    pub(crate) immutable: RwLock<VecDeque<IndexBatch<K, O::Timestamp>>>,
}

impl<K, V, O, WP> Db<K, V, O, WP>
where
    K: Ord + Send + Sync,
    V: Send,
    O: Oracle<K>,
    O::Timestamp: Send,
    WP: WalProvider + Send,
{
    pub fn new(oracle: O, wal_provider: WP, option: DbOption) -> io::Result<Self> {
        let wal_manager = Arc::new(WalManager::new(wal_provider, option.max_wal_size));
        let mutable_shards = Shard::new(|| {
            unsend::lock::RwLock::new(crate::MutableShard {
                mutable: MemTable::default(),
                wal: block_on(wal_manager.create_wal_file()).unwrap(),
            })
        });
        Ok(Db {
            option,
            oracle,
            wal_manager,
            mutable_shards,
            immutable: RwLock::new(VecDeque::new()),
        })
    }
}

impl<K, V, O, WP> Db<K, V, O, WP>
where
    K: Encode + Ord + Hash + Send + Sync + 'static,
    V: Encode + Decode + Send + 'static,
    O: Oracle<K>,
    O::Timestamp: Encode + Copy + Send + Sync + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite,
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
    ) -> Result<(), WriteError<<Record<Arc<K>, V, O::Timestamp> as Encode>::Error>> {
        let consistent_hash =
            jump_consistent_hash(fxhash::hash64(&key), executor::worker_num()) as usize;
        let wal_manager = self.wal_manager.clone();
        let freeze = self
            .mutable_shards
            .with(consistent_hash, move |local| async move {
                let mut local = local.write().await;
                let result = local
                    .wal
                    .write(Record::new(record_type, &key, &ts, value.as_ref()))
                    .await;
                match result {
                    Ok(_) => {
                        local.mutable.insert(key, ts, value);
                        Ok(None)
                    }
                    Err(e) => {
                        if let WriteError::MaxSizeExceeded = e {
                            let mut wal_file = wal_manager
                                .create_wal_file()
                                .await
                                .map_err(WriteError::Io)?;
                            let mut mem_table = MemTable::default();
                            mem_table.insert(key, ts, value);

                            mem::swap(&mut local.wal, &mut wal_file);
                            mem::swap(&mut local.mutable, &mut mem_table);

                            wal_file.close().await.map_err(WriteError::Io)?;
                            Ok(Some(mem_table))
                        } else {
                            Err(e)
                        }
                    }
                }
            })
            .await?;
        if let Some(mem_table) = freeze {
            self.immutable
                .write()
                .await
                .push_back(Self::freeze(mem_table).await?);
        }
        Ok(())
    }

    async fn get<G, F>(&self, key: &Arc<K>, ts: &O::Timestamp, f: F) -> Option<G>
    where
        G: Send + 'static,
        O::Timestamp: Sync,
        F: Fn(&V) -> G + Sync + 'static,
    {
        let consistent_hash =
            jump_consistent_hash(fxhash::hash64(key), executor::worker_num()) as usize;

        // Safety: read-only would not break data.
        let (key, ts, f) = unsafe {
            (
                mem::transmute::<_, &Arc<K>>(key),
                mem::transmute::<_, &O::Timestamp>(ts),
                mem::transmute::<_, &'static F>(&f),
            )
        };

        let mut result = self
            .mutable_shards
            .with(consistent_hash, move |local| async move {
                local.read().await.mutable.get(key, ts).map(f)
            })
            .await;
        if result.is_none() {
            let guard = self.immutable.read().await;

            for index_batch in guard.iter().rev() {
                if let Some(bytes) = index_batch.find(key, ts) {
                    if bytes.is_empty() {
                        return None;
                    }
                    let mut cursor = Cursor::new(bytes);

                    if let Ok(value) = V::decode(&mut cursor).await {
                        result = Some(f(&value));
                        break;
                    }
                }
            }
        }

        result
    }

    async fn write_batch(
        &self,
        mut kvs: impl ExactSizeIterator<Item = (Arc<K>, O::Timestamp, Option<V>)>,
    ) -> Result<(), WriteError<<Record<Arc<K>, V, O::Timestamp> as Encode>::Error>> {
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

    async fn freeze(
        mem_table: MemTable<K, V, <O as Oracle<K>>::Timestamp>,
    ) -> Result<
        IndexBatch<K, O::Timestamp>,
        WriteError<<Record<Arc<K>, V, O::Timestamp> as Encode>::Error>,
    > {
        fn clear(buf: &mut Cursor<Vec<u8>>) {
            buf.get_mut().clear();
            buf.set_position(0);
        }

        let mut buf = Cursor::new(vec![0; 128]);
        let mut index = BTreeMap::new();
        let mut key_builder = GenericBinaryBuilder::<Offset>::new();
        let mut value_builder = GenericBinaryBuilder::<Offset>::new();

        for (offset, (key, value)) in mem_table.data.into_iter().enumerate() {
            clear(&mut buf);
            key.key
                .encode(&mut buf)
                .await
                .map_err(|err| WriteError::Internal(Box::new(err)))?;
            key_builder.append_value(buf.get_ref());

            if let Some(value) = value {
                clear(&mut buf);
                value
                    .encode(&mut buf)
                    .await
                    .map_err(|err| WriteError::Internal(Box::new(err)))?;
                value_builder.append_value(buf.get_ref());
            } else {
                value_builder.append_null();
            }
            index.insert(key, offset as u32);
        }
        let keys = key_builder.finish();
        let values = value_builder.finish();

        let batch =
            RecordBatch::try_new(ELSM_SCHEMA.clone(), vec![Arc::new(keys), Arc::new(values)])
                .map_err(WriteError::Arrow)?;

        Ok(IndexBatch { batch, index })
    }
}

impl<K, V, O, WP> Oracle<K> for Db<K, V, O, WP>
where
    K: Ord,
    O: Oracle<K>,
    WP: WalProvider,
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
    fn get<G, F>(
        &self,
        key: &Arc<K>,
        ts: &Self::Timestamp,
        f: F,
    ) -> impl Future<Output = Option<G>>
    where
        G: Send + 'static,
        Self::Timestamp: Sync,
        F: Fn(&V) -> G + Sync + 'static;

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

impl<K, V, O, WP> GetWrite<K, V> for Db<K, V, O, WP>
where
    K: Encode + Ord + Hash + Send + Sync + 'static,
    V: Encode + Decode + Send + 'static,
    O: Oracle<K>,
    O::Timestamp: Encode + Copy + Send + Sync + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite,
{
    async fn write(
        &self,
        record_type: RecordType,
        key: Arc<K>,
        ts: O::Timestamp,
        value: Option<V>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::write(self, record_type, key, ts, value).await?;
        Ok(())
    }

    async fn get<G, F>(&self, key: &Arc<K>, ts: &O::Timestamp, f: F) -> Option<G>
    where
        G: Send + 'static,
        O::Timestamp: Sync,
        F: Fn(&V) -> G + Sync + 'static,
    {
        Db::get(self, key, ts, f).await
    }

    async fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<K>, O::Timestamp, Option<V>)>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::write_batch(self, kvs).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::ExecutorBuilder;
    use futures::io::Cursor;

    use crate::{
        mem_table::MemTable, oracle::LocalOracle, record::RecordType, serdes::Encode,
        transaction::CommitError, wal::provider::in_mem::InMemProvider, Db, DbOption,
    };

    #[test]
    fn read_committed() {
        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 1,
                    },
                )
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), 0);
            txn.set("key1".to_string(), 1);
            txn.commit().await.unwrap();

            let mut t0 = db.new_txn();
            let mut t1 = db.new_txn();

            t0.set(
                "key0".into(),
                t0.get(&Arc::new("key1".to_owned()), |v| *v).await.unwrap(),
            );
            t1.set(
                "key1".into(),
                t1.get(&Arc::new("key0".to_owned()), |v| *v).await.unwrap(),
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
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 1,
                    },
                )
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), 0);
            txn.set("key1".to_string(), 1);
            txn.commit().await.unwrap();

            let mut t0 = db.new_txn();
            let mut t1 = db.new_txn();
            let mut t2 = db.new_txn();

            t0.set(
                "key0".into(),
                t0.get(&Arc::new("key1".to_owned()), |v| *v).await.unwrap(),
            );
            t1.set(
                "key0".into(),
                t1.get(&Arc::new("key0".to_owned()), |v| *v).await.unwrap(),
            );
            t1.set("key2".into(), 2);
            t2.set("key2".into(), 3);

            t0.commit().await.unwrap();

            let commit = t1.commit().await;
            assert!(commit.is_err());
            assert!(t2.commit().await.is_ok());
            if let Err(CommitError::WriteConflict(keys)) = commit {
                assert_eq!(db.new_txn().get(&keys[0], |v| *v).await, Some(1));
                return;
            }
            panic!("unreachable");
        });
    }

    #[test]
    fn read_from_immut_table() {
        ExecutorBuilder::new().build().unwrap().block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let value_1 = "value_1".to_owned();
            let value_2 = "value_2".to_owned();

            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        // TIPS: kv size in test case is 17
                        max_wal_size: 20,
                        immutable_chunk_num: 1,
                    },
                )
                .unwrap(),
            );

            db.write(RecordType::Full, key_1.clone(), 0, Some(value_1.clone()))
                .await
                .unwrap();
            db.write(RecordType::Full, key_1.clone(), 1, None)
                .await
                .unwrap();
            db.write(RecordType::Full, key_2.clone(), 0, None)
                .await
                .unwrap();
            db.write(RecordType::Full, key_2.clone(), 1, Some(value_2.clone()))
                .await
                .unwrap();

            assert_eq!(db.get(&key_1, &0, |v| v.clone()).await, Some(value_1));
            assert_eq!(db.get(&key_1, &1, |v| v.clone()).await, None);
            assert_eq!(db.get(&key_2, &0, |v| v.clone()).await, None);
            assert_eq!(db.get(&key_2, &1, |v| v.clone()).await, Some(value_2));
        });
    }

    #[test]
    fn freeze() {
        fn clear(buf: &mut Cursor<Vec<u8>>) {
            buf.get_mut().clear();
            buf.set_position(0);
        }

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let key_3 = Arc::new("key_3".to_owned());
            let value_1 = "value_1".to_owned();
            let value_2 = "value_2".to_owned();

            let mut mem_table = MemTable::default();

            mem_table.insert(key_1.clone(), 0, Some(value_1.clone()));
            mem_table.insert(key_1.clone(), 1, None);
            mem_table.insert(key_2.clone(), 0, Some(value_2.clone()));
            mem_table.insert(key_3.clone(), 0, None);

            let batch = Db::<String, String, LocalOracle<String>, InMemProvider>::freeze(mem_table)
                .await
                .unwrap();

            let mut buf = Cursor::new(Vec::new());
            value_1.encode(&mut buf).await.unwrap();
            assert_eq!(batch.find(&key_1, &0), Some(buf.get_ref().as_slice()));
            assert_eq!(batch.find(&key_1, &1), Some(vec![].as_slice()));
            clear(&mut buf);
            value_2.encode(&mut buf).await.unwrap();
            assert_eq!(batch.find(&key_2, &0), Some(buf.get_ref().as_slice()));
            assert_eq!(batch.find(&key_3, &0), Some(vec![].as_slice()));
        });
    }
}
