pub(crate) mod compactor;
mod consistent_hash;
pub(crate) mod index_batch;
pub(crate) mod mem_table;
pub(crate) mod oracle;
pub(crate) mod record;
pub mod serdes;
pub mod stream;
pub mod transaction;
pub(crate) mod utils;
pub mod wal;

use std::{
    collections::{BTreeMap, VecDeque},
    error,
    fmt::Debug,
    future::Future,
    hash::Hash,
    io, mem,
    ops::DerefMut,
    path::PathBuf,
    pin::pin,
    sync::Arc,
};

use arrow::{
    array::{GenericBinaryBuilder, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_lock::{Mutex, RwLock};
use consistent_hash::jump_consistent_hash;
use executor::{
    futures::{AsyncRead, StreamExt},
    shard::Shard,
    spawn,
};
use futures::{
    channel::{
        mpsc::{channel, Sender},
        oneshot,
    },
    executor::block_on,
    io::Cursor,
    AsyncWrite,
};
use lazy_static::lazy_static;
use mem_table::MemTable;
use oracle::Oracle;
use record::{Record, RecordType};
use serdes::Encode;
use transaction::Transaction;
use wal::{provider::WalProvider, WalFile, WalManager, WalWrite, WriteError};

use crate::{
    compactor::Compactor,
    index_batch::IndexBatch,
    serdes::Decode,
    stream::{buf_stream::BufStream, merge_stream::MergeStream, EStreamImpl},
    wal::WalRecover,
};

lazy_static! {
    pub static ref ELSM_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::LargeBinary, false),
            Field::new("value", DataType::LargeBinary, true),
        ]))
    };
}

pub type Offset = i64;
pub type Immutable<K, T> = Arc<RwLock<VecDeque<IndexBatch<K, T>>>>;

#[derive(Debug)]
pub enum CompactTask {
    Flush(Option<oneshot::Sender<()>>),
}

#[derive(Debug)]
pub struct DbOption {
    pub path: PathBuf,
    pub max_wal_size: usize,
    pub immutable_chunk_num: usize,
}

#[derive(Debug)]
struct MutableShard<K, V, T>
where
    K: Ord,
    T: Ord,
{
    mutable: MemTable<K, V, T>,
}

#[derive(Debug)]
pub struct Db<K, V, O, WP>
where
    K: Ord,
    O: Oracle<K>,
    WP: WalProvider,
{
    option: Arc<DbOption>,
    pub(crate) oracle: O,
    wal_manager: Arc<WalManager<WP>>,
    pub(crate) mutable_shards: Shard<unsend::lock::RwLock<MutableShard<K, V, O::Timestamp>>>,
    pub(crate) immutable: Immutable<K, O::Timestamp>,
    #[allow(clippy::type_complexity)]
    pub(crate) wal: Arc<Mutex<WalFile<WP::File, Arc<K>, V, O::Timestamp>>>,
    pub(crate) compaction_tx: Mutex<Sender<CompactTask>>,
}

impl<K, V, O, WP> Db<K, V, O, WP>
where
    K: Encode + Decode + Ord + Hash + Send + Sync + 'static,
    V: Encode + Decode + Send + Sync + 'static,
    O: Oracle<K> + 'static,
    O::Timestamp: Encode + Decode + Copy + Send + Sync + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite + AsyncRead,
    io::Error: From<<V as Decode>::Error>,
{
    pub async fn new(
        oracle: O,
        wal_provider: WP,
        option: DbOption,
    ) -> Result<Self, WriteError<<Record<Arc<K>, V, O::Timestamp> as Encode>::Error>> {
        let wal_manager = Arc::new(WalManager::new(wal_provider, option.max_wal_size));
        let mutable_shards = Shard::new(|| {
            unsend::lock::RwLock::new(crate::MutableShard {
                mutable: MemTable::default(),
            })
        });
        let wal = Arc::new(Mutex::new(block_on(wal_manager.create_wal_file()).unwrap()));

        let immutable = Arc::new(RwLock::new(VecDeque::new()));
        let option = Arc::new(option);

        let (task_tx, mut task_rx) = channel(1);
        let mut compactor = Compactor::<K, O>::new(immutable.clone(), option.clone());

        spawn(async move {
            loop {
                match task_rx.next().await {
                    None => break,
                    Some(task) => match task {
                        CompactTask::Flush(option_tx) => {
                            if let Err(err) = compactor.check_then_compaction(option_tx).await {
                                println!("[Compaction Error]: {}", err)
                            }
                        }
                    },
                }
            }
        })
        .detach();

        let mut db = Db {
            option,
            oracle,
            wal_manager: wal_manager.clone(),
            mutable_shards,
            immutable,
            wal,
            compaction_tx: Mutex::new(task_tx),
        };
        let mut file_stream = pin!(wal_manager.wal_provider.list());

        while let Some(file) = file_stream.next().await {
            let file = file.map_err(|err| WriteError::Internal(Box::new(err)))?;

            db.recover(
                &mut wal_manager
                    .pack_wal_file(file)
                    .await
                    .map_err(WriteError::Io)?,
            )
            .await
            .map_err(|err| WriteError::Internal(Box::new(err)))?;
        }

        Ok(db)
    }
}

impl<K, V, O, WP> Db<K, V, O, WP>
where
    K: Encode + Ord + Hash + Send + Sync + 'static,
    V: Encode + Decode + Send + Sync + 'static,
    O: Oracle<K>,
    O::Timestamp: Encode + Copy + Send + Sync + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite,
    io::Error: From<<V as Decode>::Error>,
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
        let wal = self.wal.clone();
        let freeze = self
            .mutable_shards
            .with(consistent_hash, move |local| async move {
                let mut local = local.write().await;
                let result = wal
                    .lock()
                    .await
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
                            {
                                let mut guard = wal.lock().await;
                                mem::swap(guard.deref_mut(), &mut wal_file);
                            }
                            wal_file.close().await.map_err(WriteError::Io)?;
                            let mut mem_table = MemTable::default();
                            mem_table.insert(key, ts, value);

                            mem::swap(&mut local.mutable, &mut mem_table);

                            Ok(Some(mem_table))
                        } else {
                            Err(e)
                        }
                    }
                }
            })
            .await?;

        let mut is_exceeded = false;
        if let Some(mem_table) = freeze {
            let mut guard = self.immutable.write().await;

            guard.push_back(Self::freeze(mem_table).await?);
            is_exceeded = guard.len() > self.option.immutable_chunk_num;
        }
        if is_exceeded {
            if let Some(mut guard) = self.compaction_tx.try_lock() {
                let _ = guard.try_send(CompactTask::Flush(None));
            }
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

        if let Some(value) = self
            .mutable_shards
            .with(consistent_hash, move |local| async move {
                local.read().await.mutable.get(key, ts).map(|v| v.map(f))
            })
            .await
        {
            return value;
        }
        let guard = self.immutable.read().await;

        for index_batch in guard.iter().rev() {
            if let Ok(Some(value)) = index_batch.find(key, ts).await {
                return value.map(|v| f(&v));
            }
        }
        None
    }

    async fn range<G, F>(
        &self,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
        ts: &O::Timestamp,
        f: F,
    ) -> Result<MergeStream<K, O::Timestamp, V, G, F>, <V as Decode>::Error>
    where
        G: Send + Sync + 'static,
        F: Fn(&V) -> G + Sync + Send + 'static + Copy,
        O::Timestamp: Sync,
    {
        let iters = self.inner_range(lower, upper, ts, f).await?;

        MergeStream::new(iters).await
    }

    pub(crate) async fn inner_range<'s, G, F>(
        &'s self,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
        ts: &<O as Oracle<K>>::Timestamp,
        f: F,
    ) -> Result<Vec<EStreamImpl<K, O::Timestamp, V, G, F>>, <V as Decode>::Error>
    where
        G: Send + Sync + 'static,
        F: Fn(&V) -> G + Sync + Send + 'static + Copy,
    {
        let mut iters = futures::future::try_join_all((0..executor::worker_num()).map(|i| {
            let lower = lower.cloned();
            let upper = upper.cloned();
            let ts = *ts;

            self.mutable_shards.with(i, move |local| async move {
                let guard = local.read().await;
                let mut items = Vec::new();

                let mut iter = pin!(
                    guard
                        .mutable
                        .range(lower.as_ref(), upper.as_ref(), &ts, f)
                        .await?,
                );

                while let Some(item) = iter.next().await {
                    let (k, v) = item?;

                    items.push((k.clone(), v));
                }
                Ok(EStreamImpl::Buf(BufStream::new(items)))
            })
        }))
        .await?;
        let guard = self.immutable.read().await;

        for batch in guard.iter() {
            let mut items = Vec::new();

            let mut stream = pin!(batch.range(lower, upper, ts, f).await?);

            while let Some(item) = stream.next().await {
                let (k, v) = item?;

                items.push((k.clone(), v));
            }
            iters.push(EStreamImpl::Buf(BufStream::new(items)));
        }
        Ok(iters)
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

    async fn recover<W>(
        &mut self,
        wal: &mut W,
    ) -> Result<(), WriteError<<Record<Arc<K>, V, O::Timestamp> as Encode>::Error>>
    where
        W: WalRecover<Arc<K>, V, O::Timestamp>,
    {
        let mut stream = pin!(wal.recover());
        while let Some(record) = stream.next().await {
            let mut record_type = RecordType::First;
            let Record { key, ts, value, .. } =
                record.map_err(|err| WriteError::Internal(Box::new(err)))?;

            self.write(
                mem::replace(&mut record_type, RecordType::Middle),
                key,
                ts,
                value,
            )
            .await?;
        }
        Ok(())
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

pub(crate) trait GetWrite<K, V>: Oracle<K>
where
    K: Ord,
    V: Decode,
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

    fn inner_range<'a, G, F>(
        &'a self,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
        ts: &Self::Timestamp,
        f: F,
    ) -> impl Future<
        Output = Result<Vec<EStreamImpl<'a, K, Self::Timestamp, V, G, F>>, <V as Decode>::Error>,
    >
    where
        K: 'a,
        Self::Timestamp: 'a,
        V: 'a,
        G: Send + Sync + 'static,
        F: Fn(&V) -> G + Sync + Send + 'static + Copy;
}

impl<K, V, O, WP> GetWrite<K, V> for Db<K, V, O, WP>
where
    K: Encode + Ord + Hash + Send + Sync + 'static,
    V: Encode + Decode + Send + Sync + 'static,
    O: Oracle<K>,
    O::Timestamp: Encode + Copy + Send + Sync + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite,
    io::Error: From<<V as Decode>::Error>,
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

    async fn inner_range<'a, G, F>(
        &'a self,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
        ts: &<O as Oracle<K>>::Timestamp,
        f: F,
    ) -> Result<Vec<EStreamImpl<'a, K, O::Timestamp, V, G, F>>, <V as Decode>::Error>
    where
        K: 'a,
        Self::Timestamp: 'a,
        V: 'a,
        G: Send + Sync + 'static,
        F: Fn(&V) -> G + Sync + Send + 'static + Copy,
    {
        Db::inner_range(self, lower, upper, ts, f).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::{futures::StreamExt, ExecutorBuilder};
    use tempfile::TempDir;

    use crate::{
        oracle::LocalOracle,
        record::RecordType,
        transaction::CommitError,
        wal::provider::{fs::Fs, in_mem::InMemProvider},
        Db, DbOption,
    };

    #[test]
    fn read_committed() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        path: temp_dir.path().to_path_buf(),
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 5,
                    },
                )
                .await
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
    fn range() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        path: temp_dir.path().to_path_buf(),
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 5,
                    },
                )
                .await
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), 0);
            txn.set("key1".to_string(), 1);
            txn.set("key2".to_string(), 2);
            txn.set("key3".to_string(), 3);
            txn.commit().await.unwrap();

            let mut iter = db
                .range(
                    Some(&Arc::new("key1".to_string())),
                    Some(&Arc::new("key2".to_string())),
                    &1,
                    |v| *v,
                )
                .await
                .unwrap();

            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new("key1".to_string()), Some(1))
            );
            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new("key2".to_string()), Some(2))
            );

            let mut txn_1 = db.new_txn();
            txn_1.set("key5".to_string(), 5);
            txn_1.set("key4".to_string(), 4);

            let mut txn_2 = db.new_txn();
            txn_2.set("key5".to_string(), 4);
            txn_2.set("key4".to_string(), 5);
            txn_2.commit().await.unwrap();

            let mut iter = txn_1
                .range(
                    Some(&Arc::new("key1".to_string())),
                    Some(&Arc::new("key4".to_string())),
                    |v| *v,
                )
                .await
                .unwrap();

            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new("key1".to_string()), Some(1))
            );
            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new("key2".to_string()), Some(2))
            );
            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new("key3".to_string()), Some(3))
            );
            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new("key4".to_string()), Some(4))
            );
        });
    }

    #[test]
    fn write_conflicts() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        path: temp_dir.path().to_path_buf(),
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 5,
                    },
                )
                .await
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
        let temp_dir = TempDir::new().unwrap();

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
                        path: temp_dir.path().to_path_buf(),
                        max_wal_size: 20,
                        immutable_chunk_num: 5,
                    },
                )
                .await
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
    fn recover_from_wal() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    Fs::new(temp_dir.path()).unwrap(),
                    DbOption {
                        path: temp_dir.path().to_path_buf(),
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 5,
                    },
                )
                .await
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), "value0".to_string());
            txn.set("key1".to_string(), "value1".to_string());
            txn.commit().await.unwrap();

            drop(db);

            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    Fs::new(temp_dir.path()).unwrap(),
                    DbOption {
                        path: temp_dir.path().to_path_buf(),
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 5,
                    },
                )
                .await
                .unwrap(),
            );

            assert_eq!(
                db.get(&Arc::new("key0".to_string()), &1, |v: &String| v.clone())
                    .await,
                Some("value0".to_string()),
            );
            assert_eq!(
                db.get(&Arc::new("key1".to_string()), &1, |v: &String| v.clone())
                    .await,
                Some("value1".to_string()),
            );
        });
    }
}
