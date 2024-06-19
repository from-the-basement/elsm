mod compactor;
mod consistent_hash;
pub(crate) mod index_batch;
pub(crate) mod mem_table;
pub(crate) mod oracle;
pub(crate) mod record;
pub(crate) mod schema;
pub(crate) mod scope;
pub mod serdes;
pub mod stream;
pub mod transaction;
pub(crate) mod user;
pub(crate) mod utils;
mod version;
pub mod wal;

use std::{
    collections::{BTreeMap, VecDeque},
    error,
    fmt::Debug,
    future::Future,
    io, mem,
    ops::DerefMut,
    path::PathBuf,
    pin::pin,
    sync::Arc,
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
    AsyncWrite,
};
use mem_table::MemTable;
use oracle::Oracle;
use record::{Record, RecordType};
use serdes::Encode;
use snowflake::ProcessUniqueId;
use tracing::error;
use transaction::Transaction;
use wal::{provider::WalProvider, WalFile, WalManager, WalWrite, WriteError};

use crate::{
    compactor::Compactor,
    index_batch::IndexBatch,
    oracle::TimeStamp,
    schema::Builder,
    serdes::Decode,
    stream::{buf_stream::BufStream, merge_stream::MergeStream, EStreamImpl, StreamError},
    version::{cleaner::Cleaner, set::VersionSet, Version},
    wal::WalRecover,
};

pub type Offset = i64;
pub(crate) type Immutable<S> = Arc<RwLock<VecDeque<IndexBatch<S>>>>;

#[derive(Debug)]
pub enum CompactTask {
    Flush(Option<oneshot::Sender<()>>),
}

#[derive(Debug)]
pub struct DbOption {
    pub path: PathBuf,
    pub max_mem_table_size: usize,
    pub immutable_chunk_num: usize,
    pub major_threshold_with_sst_size: usize,
    pub level_sst_magnification: usize,
    pub max_sst_file_size: usize,
    pub clean_channel_buffer: usize,
}

#[derive(Debug)]
struct MutableShard<S>
where
    S: schema::Schema,
{
    mutable: MemTable<S>,
}

pub struct Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey>,
    WP: WalProvider,
{
    option: Arc<DbOption>,
    pub(crate) oracle: O,
    wal_manager: Arc<WalManager<WP>>,
    pub(crate) mutable_shards: Shard<unsend::lock::RwLock<MutableShard<S>>>,
    pub(crate) immutable: Immutable<S>,
    #[allow(clippy::type_complexity)]
    pub(crate) wal: Arc<Mutex<WalFile<WP::File, Arc<S::PrimaryKey>, S>>>,
    pub(crate) compaction_tx: Mutex<Sender<CompactTask>>,
    pub(crate) version_set: VersionSet<S>,
}

impl<S, O, WP> Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey> + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite + AsyncRead,
    io::Error: From<<S as Decode>::Error>,
{
    pub async fn new(
        oracle: O,
        wal_provider: WP,
        option: DbOption,
    ) -> Result<Self, WriteError<<Record<Arc<S::PrimaryKey>, S> as Encode>::Error>> {
        let wal_manager = Arc::new(WalManager::new(wal_provider));
        let mutable_shards = Shard::new(|| {
            unsend::lock::RwLock::new(crate::MutableShard {
                mutable: MemTable::default(),
            })
        });
        let wal = Arc::new(Mutex::new(block_on(wal_manager.create_wal_file()).unwrap()));

        let immutable = Arc::new(RwLock::new(VecDeque::new()));
        let option = Arc::new(option);

        let (task_tx, mut task_rx) = channel(1);
        let (mut cleaner, clean_sender) = Cleaner::new(option.clone());

        let version_set = VersionSet::<S>::new(&option, clean_sender.clone())
            .await
            .unwrap();
        let mut compactor =
            Compactor::<S>::new(immutable.clone(), option.clone(), version_set.clone());

        spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        })
        .detach();
        spawn(async move {
            loop {
                match task_rx.next().await {
                    None => break,
                    Some(task) => match task {
                        CompactTask::Flush(option_tx) => {
                            if let Err(err) = compactor.check_then_compaction(option_tx).await {
                                error!("[Compaction Error]: {}", err)
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
            version_set,
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

impl<S, O, WP> Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey>,
    WP: WalProvider,
    WP::File: AsyncWrite,
    io::Error: From<<S as Decode>::Error>,
{
    pub fn new_txn(self: &Arc<Self>) -> Transaction<S, Self> {
        Transaction::new(self.clone())
    }

    async fn write(
        &self,
        record_type: RecordType,
        key: Arc<S::PrimaryKey>,
        ts: TimeStamp,
        value: Option<S>,
    ) -> Result<(), WriteError<<Record<Arc<S::PrimaryKey>, S> as Encode>::Error>> {
        let consistent_hash =
            jump_consistent_hash(fxhash::hash64(&key), executor::worker_num()) as usize;
        let wal_manager = self.wal_manager.clone();
        let wal = self.wal.clone();
        let max_mem_table_size = self.option.max_mem_table_size;

        let freeze = self
            .mutable_shards
            .with(consistent_hash, move |local| async move {
                let mut local = local.write().await;
                wal.lock()
                    .await
                    .write(Record::new(record_type, &key, ts, value.as_ref()))
                    .await?;

                local.mutable.insert(key, ts, value);
                if local.mutable.is_excess(max_mem_table_size) {
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

                    mem::swap(&mut local.mutable, &mut mem_table);

                    return Ok::<
                        Option<MemTable<S>>,
                        WriteError<<Record<&S::PrimaryKey, &S> as Encode>::Error>,
                    >(Some(mem_table));
                }
                Ok(None)
            })
            .await?;

        if let Some(mem_table) = freeze {
            if mem_table.is_empty() {
                return Ok(());
            }
            let mut guard = self.immutable.write().await;

            guard.push_back(Self::freeze(mem_table).await?);
            if guard.len() > self.option.immutable_chunk_num {
                if let Some(mut guard) = self.compaction_tx.try_lock() {
                    let _ = guard.try_send(CompactTask::Flush(None));
                }
            }
        }
        Ok(())
    }

    async fn get<G, F>(&self, key: &Arc<S::PrimaryKey>, ts: &TimeStamp, f: F) -> Option<G>
    where
        G: Send + 'static,
        F: Fn(&S) -> G + Sync + 'static,
    {
        let consistent_hash =
            jump_consistent_hash(fxhash::hash64(key), executor::worker_num()) as usize;

        // Safety: read-only would not break data.
        let (key, ts, f) = unsafe {
            (
                mem::transmute::<_, &Arc<S::PrimaryKey>>(key),
                mem::transmute::<_, &TimeStamp>(ts),
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
            if let Some(value) = index_batch.find(key, ts).await {
                return value.map(|v| f(&v));
            }
        }
        drop(guard);

        let guard = self.version_set.current().await;
        if let Ok(Some(record_batch)) = guard.query(key, &self.option).await {
            return S::from_batch(&record_batch, 0).1.map(|v| f(&v));
        }
        drop(guard);

        None
    }

    async fn range<G, F>(
        &self,
        lower: Option<&Arc<S::PrimaryKey>>,
        upper: Option<&Arc<S::PrimaryKey>>,
        ts: &TimeStamp,
        f: F,
    ) -> Result<MergeStream<S, G, F>, StreamError<S::PrimaryKey, S>>
    where
        G: Send + Sync + 'static,
        F: Fn(&S) -> G + Sync + Send + 'static + Copy,
    {
        let iters = self.inner_range(lower, upper, ts, f).await?;

        MergeStream::new(iters).await
    }

    pub(crate) async fn inner_range<'s, G, F>(
        &'s self,
        lower: Option<&Arc<S::PrimaryKey>>,
        upper: Option<&Arc<S::PrimaryKey>>,
        ts: &TimeStamp,
        f: F,
    ) -> Result<Vec<EStreamImpl<S, G, F>>, StreamError<S::PrimaryKey, S>>
    where
        G: Send + Sync + 'static,
        F: Fn(&S) -> G + Sync + Send + 'static + Copy,
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
        mut kvs: impl ExactSizeIterator<Item = (Arc<S::PrimaryKey>, TimeStamp, Option<S>)>,
    ) -> Result<(), WriteError<<Record<Arc<S::PrimaryKey>, S> as Encode>::Error>> {
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
        mem_table: MemTable<S>,
    ) -> Result<IndexBatch<S>, WriteError<<Record<Arc<S::PrimaryKey>, S> as Encode>::Error>> {
        let mut index = BTreeMap::new();

        let mut builder = S::builder();

        for (offset, (key, value)) in mem_table.data.into_iter().enumerate() {
            builder.add(&key.key, value);
            index.insert(key, offset as u32);
        }
        let batch = builder.finish();

        Ok(IndexBatch { batch, index })
    }

    async fn recover<W>(
        &mut self,
        wal: &mut W,
    ) -> Result<(), WriteError<<Record<Arc<S::PrimaryKey>, S> as Encode>::Error>>
    where
        W: WalRecover<Arc<S::PrimaryKey>, S>,
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

impl<S, O, WP> Oracle<S::PrimaryKey> for Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey>,
    WP: WalProvider,
{
    fn start_read(&self) -> TimeStamp {
        self.oracle.start_read()
    }

    fn read_commit(&self, ts: TimeStamp) {
        self.oracle.read_commit(ts)
    }

    fn start_write(&self) -> TimeStamp {
        self.oracle.start_write()
    }

    fn write_commit(
        &self,
        read_at: TimeStamp,
        write_at: TimeStamp,
        in_write: std::collections::HashSet<Arc<S::PrimaryKey>>,
    ) -> Result<(), oracle::WriteConflict<S::PrimaryKey>> {
        self.oracle.write_commit(read_at, write_at, in_write)
    }
}

pub(crate) trait GetWrite<S>: Oracle<S::PrimaryKey>
where
    S: schema::Schema,
{
    fn get<G, F>(
        &self,
        key: &Arc<S::PrimaryKey>,
        ts: &TimeStamp,
        f: F,
    ) -> impl Future<Output = Option<G>>
    where
        G: Send + 'static,
        TimeStamp: Sync,
        F: Fn(&S) -> G + Sync + 'static;

    fn write(
        &self,
        record_type: RecordType,
        key: Arc<S::PrimaryKey>,
        ts: TimeStamp,
        value: Option<S>,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync + 'static>>>;

    fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<S::PrimaryKey>, TimeStamp, Option<S>)>,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync + 'static>>>;

    fn inner_range<'a, G, F>(
        &'a self,
        lower: Option<&Arc<S::PrimaryKey>>,
        upper: Option<&Arc<S::PrimaryKey>>,
        ts: &TimeStamp,
        f: F,
    ) -> impl Future<Output = Result<Vec<EStreamImpl<'a, S, G, F>>, StreamError<S::PrimaryKey, S>>>
    where
        S::PrimaryKey: 'a,
        TimeStamp: 'a,
        S: 'a,
        G: Send + Sync + 'static,
        F: Fn(&S) -> G + Sync + Send + 'static + Copy;
}

impl<S, O, WP> GetWrite<S> for Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey>,
    WP: WalProvider,
    WP::File: AsyncWrite,
    io::Error: From<<S as Decode>::Error>,
{
    async fn write(
        &self,
        record_type: RecordType,
        key: Arc<S::PrimaryKey>,
        ts: TimeStamp,
        value: Option<S>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::write(self, record_type, key, ts, value).await?;
        Ok(())
    }

    async fn get<G, F>(&self, key: &Arc<S::PrimaryKey>, ts: &TimeStamp, f: F) -> Option<G>
    where
        G: Send + 'static,
        F: Fn(&S) -> G + Sync + 'static,
    {
        Db::get(self, key, ts, f).await
    }

    async fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<S::PrimaryKey>, TimeStamp, Option<S>)>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::write_batch(self, kvs).await?;
        Ok(())
    }

    async fn inner_range<'a, G, F>(
        &'a self,
        lower: Option<&Arc<S::PrimaryKey>>,
        upper: Option<&Arc<S::PrimaryKey>>,
        ts: &TimeStamp,
        f: F,
    ) -> Result<Vec<EStreamImpl<'a, S, G, F>>, StreamError<S::PrimaryKey, S>>
    where
        S::PrimaryKey: 'a,
        TimeStamp: 'a,
        S: 'a,
        G: Send + Sync + 'static,
        F: Fn(&S) -> G + Sync + Send + 'static + Copy,
    {
        Db::inner_range(self, lower, upper, ts, f).await
    }
}

impl DbOption {
    pub(crate) fn new(path: impl Into<PathBuf> + Send) -> Self {
        DbOption {
            path: path.into(),
            max_mem_table_size: 8 * 1024 * 1024,
            immutable_chunk_num: 5,
            major_threshold_with_sst_size: 10,
            level_sst_magnification: 10,
            max_sst_file_size: 64 * 1024 * 1024,
            clean_channel_buffer: 10,
        }
    }

    pub(crate) fn table_path(&self, gen: &ProcessUniqueId) -> PathBuf {
        self.path.join(format!("{}.parquet", gen))
    }
    pub(crate) fn version_path(&self) -> PathBuf {
        self.path.join("version.log")
    }

    pub(crate) fn is_threshold_exceeded_major<S>(&self, version: &Version<S>, level: usize) -> bool
    where
        S: schema::Schema,
    {
        version.tables_len(level)
            >= (self.major_threshold_with_sst_size * self.level_sst_magnification.pow(level as u32))
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
        user::User,
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
                    DbOption::new(temp_dir.path().to_path_buf()),
                )
                .await
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set(0, User::new(0, "0".to_string()));
            txn.set(1, User::new(1, "1".to_string()));
            txn.commit().await.unwrap();

            let mut t0 = db.new_txn();
            let mut t1 = db.new_txn();

            t0.set(0, t0.get(&Arc::new(1), |v| v.clone()).await.unwrap());
            t1.set(1, t1.get(&Arc::new(0), |v| v.clone()).await.unwrap());

            t0.commit().await.unwrap();
            t1.commit().await.unwrap();

            let txn = db.new_txn();

            assert_eq!(
                txn.get(&Arc::from(0), |v| v.clone()).await,
                Some(User::new(1, "1".to_string()))
            );
            assert_eq!(
                txn.get(&Arc::from(1), |v| v.clone()).await,
                Some(User::new(0, "0".to_string()))
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
                    DbOption::new(temp_dir.path().to_path_buf()),
                )
                .await
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set(0, User::new(0, "0".to_string()));
            txn.set(1, User::new(1, "1".to_string()));
            txn.set(2, User::new(2, "2".to_string()));
            txn.set(3, User::new(3, "3".to_string()));
            txn.commit().await.unwrap();

            let mut iter = db
                .range(Some(&Arc::new(1)), Some(&Arc::new(2)), &1, |v| v.clone())
                .await
                .unwrap();

            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new(1), Some(User::new(1, "1".to_string())))
            );
            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new(2), Some(User::new(2, "2".to_string())))
            );

            let mut txn_1 = db.new_txn();
            txn_1.set(5, User::new(5, "5".to_string()));
            txn_1.set(4, User::new(4, "4".to_string()));

            let mut txn_2 = db.new_txn();
            txn_2.set(5, User::new(4, "4".to_string()));
            txn_2.set(4, User::new(5, "5".to_string()));
            txn_2.commit().await.unwrap();

            let mut iter = txn_1
                .range(Some(&Arc::new(1)), Some(&Arc::new(4)), |v| v.clone())
                .await
                .unwrap();

            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new(1), Some(User::new(1, "1".to_string())))
            );
            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new(2), Some(User::new(2, "2".to_string())))
            );
            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new(3), Some(User::new(3, "3".to_string())))
            );
            assert_eq!(
                iter.next().await.unwrap().unwrap(),
                (Arc::new(4), Some(User::new(4, "4".to_string())))
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
                    DbOption::new(temp_dir.path().to_path_buf()),
                )
                .await
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set(0, User::new(0, "0".to_string()));
            txn.set(1, User::new(1, "1".to_string()));
            txn.commit().await.unwrap();

            let mut t0 = db.new_txn();
            let mut t1 = db.new_txn();
            let mut t2 = db.new_txn();

            t0.set(0, t0.get(&Arc::new(1), |v| v.clone()).await.unwrap());
            t1.set(0, t1.get(&Arc::new(0), |v| v.clone()).await.unwrap());
            t1.set(2, User::new(2, "2".to_string()));
            t2.set(2, User::new(3, "3".to_string()));

            t0.commit().await.unwrap();

            let commit = t1.commit().await;
            assert!(commit.is_err());
            assert!(t2.commit().await.is_ok());
            if let Err(CommitError::WriteConflict(keys)) = commit {
                assert_eq!(
                    db.new_txn().get(&keys[0], |v| v.clone()).await,
                    Some(User::new(1, "1".to_string()))
                );
                return;
            }
            panic!("unreachable");
        });
    }

    #[test]
    fn read_from_disk() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        // TIPS: kv size in test case is 17
                        path: temp_dir.path().to_path_buf(),
                        max_mem_table_size: 25,
                        immutable_chunk_num: 1,
                        major_threshold_with_sst_size: 5,
                        level_sst_magnification: 10,
                        max_sst_file_size: 2 * 1024 * 1024,
                        clean_channel_buffer: 10,
                    },
                )
                .await
                .unwrap(),
            );

            db.write(
                RecordType::Full,
                Arc::new(1),
                0,
                Some(User::new(1, "1".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(2),
                0,
                Some(User::new(2, "2".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(3),
                0,
                Some(User::new(3, "3".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(4),
                0,
                Some(User::new(4, "4".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(5),
                0,
                Some(User::new(5, "5".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(6),
                0,
                Some(User::new(6, "6".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(7),
                0,
                Some(User::new(7, "7".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(8),
                0,
                Some(User::new(8, "8".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(9),
                0,
                Some(User::new(9, "9".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(10),
                0,
                Some(User::new(10, "10".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(20),
                0,
                Some(User::new(20, "20".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(30),
                0,
                Some(User::new(30, "30".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(40),
                0,
                Some(User::new(40, "40".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(50),
                0,
                Some(User::new(50, "50".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(60),
                0,
                Some(User::new(60, "60".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(70),
                0,
                Some(User::new(70, "70".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(80),
                0,
                Some(User::new(80, "80".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(90),
                0,
                Some(User::new(90, "90".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(100),
                0,
                Some(User::new(100, "100".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(200),
                0,
                Some(User::new(200, "200".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(300),
                0,
                Some(User::new(300, "300".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(400),
                0,
                Some(User::new(400, "400".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(500),
                0,
                Some(User::new(500, "500".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(600),
                0,
                Some(User::new(600, "600".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(700),
                0,
                Some(User::new(700, "700".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(800),
                0,
                Some(User::new(800, "800".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(900),
                0,
                Some(User::new(900, "900".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(1000),
                0,
                Some(User::new(1000, "1000".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(2000),
                0,
                Some(User::new(2000, "2000".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(3000),
                0,
                Some(User::new(3000, "3000".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(4000),
                0,
                Some(User::new(4000, "4000".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(5000),
                0,
                Some(User::new(5000, "5000".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(6000),
                0,
                Some(User::new(6000, "6000".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(7000),
                0,
                Some(User::new(7000, "7000".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(8000),
                0,
                Some(User::new(8000, "8000".to_string())),
            )
            .await
            .unwrap();
            db.write(
                RecordType::Full,
                Arc::new(9000),
                0,
                Some(User::new(9000, "9000".to_string())),
            )
            .await
            .unwrap();

            assert_eq!(
                db.get(&Arc::new(20), &0, |v| v.clone()).await,
                Some(User::new(20, "20".to_string()))
            );

            drop(db);

            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        // TIPS: kv size in test case is 17
                        path: temp_dir.path().to_path_buf(),
                        max_mem_table_size: 25,
                        immutable_chunk_num: 1,
                        major_threshold_with_sst_size: 5,
                        level_sst_magnification: 10,
                        max_sst_file_size: 2 * 1024 * 1024,
                        clean_channel_buffer: 10,
                    },
                )
                .await
                .unwrap(),
            );

            assert_eq!(
                db.get(&Arc::new(20), &0, |v: &User| v.clone()).await,
                Some(User::new(20, "20".to_string()))
            );
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
                    DbOption::new(temp_dir.path().to_path_buf()),
                )
                .await
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set(0, User::new(0, "0".to_string()));
            txn.set(1, User::new(1, "1".to_string()));
            txn.commit().await.unwrap();

            drop(db);

            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    Fs::new(temp_dir.path()).unwrap(),
                    DbOption::new(temp_dir.path().to_path_buf()),
                )
                .await
                .unwrap(),
            );

            assert_eq!(
                db.get(&Arc::new(0), &1, |v: &User| v.clone()).await,
                Some(User::new(0, "0".to_string())),
            );
            assert_eq!(
                db.get(&Arc::new(1), &1, |v: &User| v.clone()).await,
                Some(User::new(1, "1".to_string())),
            );
        });
    }
}
