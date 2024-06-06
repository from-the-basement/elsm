use std::{collections::VecDeque, fmt::Debug, fs::File, marker::PhantomData, pin::pin, sync::Arc};

use arrow::{
    array::{GenericBinaryBuilder, GenericByteBuilder, RecordBatch},
    datatypes::GenericBinaryType,
};
use executor::futures::{util::io::Cursor, StreamExt};
use futures::channel::oneshot;
use parquet::arrow::ArrowWriter;
use snowflake::ProcessUniqueId;
use thiserror::Error;

use crate::{
    index_batch::IndexBatch,
    oracle::Oracle,
    scope::Scope,
    serdes::{Decode, Encode},
    stream::{
        merge_inner_stream::MergeInnerStream, table_stream::TableStream, EInnerStreamImpl,
        StreamError,
    },
    version::{apply_edits, edit::VersionEdit, SyncVersion, Version, VersionError, MAX_LEVEL},
    DbOption, Immutable, Offset, ELSM_SCHEMA,
};

pub(crate) struct Compactor<K, O, V>
where
    K: Ord + Debug + Encode + Decode + Send + Sync + 'static,
    V: Debug + Encode + Decode + Send + Sync + 'static,
    O: Oracle<K>,
{
    pub(crate) option: Arc<DbOption>,
    pub(crate) immutable: Immutable<K, O::Timestamp>,
    pub(crate) version: SyncVersion<K>,
    _p: PhantomData<V>,
}

impl<K, O, V> Compactor<K, O, V>
where
    K: Ord + Debug + Encode + Decode + Send + Sync + 'static,
    V: Debug + Encode + Decode + Send + Sync + 'static,
    O: Oracle<K>,
{
    pub(crate) fn new(
        immutable: Immutable<K, O::Timestamp>,
        option: Arc<DbOption>,
        version: SyncVersion<K>,
    ) -> Self {
        Compactor::<K, O, V> {
            option,
            immutable,
            version,
            _p: Default::default(),
        }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        option_tx: Option<oneshot::Sender<()>>,
    ) -> Result<(), CompactionError<K, V>> {
        let mut guard = self.immutable.write().await;

        if guard.len() > self.option.immutable_chunk_num {
            let excess = guard.split_off(self.option.immutable_chunk_num);

            if let Some(scope) = Self::minor_compaction(&self.option, excess).await? {
                let mut guard = self.version.write().await;

                let mut version_edits = vec![];

                if self.option.is_threshold_exceeded_major(&guard, 0) {
                    Self::major_compaction(
                        &guard,
                        &self.option,
                        &scope.min,
                        &scope.max,
                        &mut version_edits,
                    )
                    .await?;
                }
                version_edits.insert(0, VersionEdit::Add { level: 0, scope });

                apply_edits(&mut guard, version_edits, false)
                    .await
                    .map_err(CompactionError::Version)?;
            }
        }
        if let Some(tx) = option_tx {
            let _ = tx.send(());
        }
        Ok(())
    }

    pub(crate) async fn minor_compaction(
        option: &DbOption,
        batches: VecDeque<IndexBatch<K, O::Timestamp>>,
    ) -> Result<Option<Scope<K>>, CompactionError<K, V>> {
        if !batches.is_empty() {
            let mut min = None;
            let mut max = None;

            let gen = ProcessUniqueId::new();

            // FIXME: Async Writer
            let mut writer = ArrowWriter::try_new(
                File::create(option.table_path(&gen)).map_err(CompactionError::Io)?,
                ELSM_SCHEMA.clone(),
                None,
            )
            .map_err(CompactionError::Parquet)?;

            for batch in batches {
                if let Some((batch_min, batch_max)) = batch.scope() {
                    if matches!(min.as_ref().map(|min| min > batch_min), Some(true) | None) {
                        min = Some(batch_min.clone())
                    }
                    if matches!(max.as_ref().map(|max| max < batch_max), Some(true) | None) {
                        max = Some(batch_max.clone())
                    }
                }
                writer
                    .write(&batch.batch)
                    .map_err(CompactionError::Parquet)?;
            }
            writer.close().map_err(CompactionError::Parquet)?;
            return Ok(Some(Scope {
                min: min.unwrap(),
                max: max.unwrap(),
                gen,
            }));
        }
        Ok(None)
    }

    pub(crate) async fn major_compaction(
        version: &Version<K>,
        option: &DbOption,
        mut min: &Arc<K>,
        mut max: &Arc<K>,
        version_edits: &mut Vec<VersionEdit<K>>,
    ) -> Result<(), CompactionError<K, V>> {
        fn clear(buf: &mut Cursor<Vec<u8>>) {
            buf.get_mut().clear();
            buf.set_position(0);
        }

        let mut level = 0;

        while level < MAX_LEVEL - 2 {
            if !option.is_threshold_exceeded_major(version, level) {
                break;
            }

            let mut meet_scopes_l = Vec::new();
            {
                let index = Version::<K>::scope_search(min, &version.level_slice[level]);

                for scope in version.level_slice[level][index..].iter() {
                    if scope.is_between(min) || scope.is_between(max) {
                        meet_scopes_l.push(scope);
                    }
                }
                if meet_scopes_l.is_empty() {
                    return Ok(());
                }
            }
            let mut meet_scopes_ll = Vec::new();
            {
                if !version.level_slice[level + 1].is_empty() {
                    let min_key = &meet_scopes_l.first().unwrap().min;
                    let max_key = &meet_scopes_l.last().unwrap().max;
                    min = min_key;
                    max = max_key;

                    let min_index =
                        Version::<K>::scope_search(min_key, &version.level_slice[level]);
                    let max_index =
                        Version::<K>::scope_search(max_key, &version.level_slice[level]);

                    for scope in version.level_slice[level + 1][min_index..max_index].iter() {
                        if scope.is_between(min) || scope.is_between(max) {
                            meet_scopes_ll.push(scope);
                        }
                    }
                }
            }
            let mut streams = Vec::with_capacity(meet_scopes_l.len() + meet_scopes_ll.len());

            for scope in meet_scopes_l.iter().chain(meet_scopes_ll.iter()) {
                streams.push(EInnerStreamImpl::Table(
                    TableStream::new(option, &scope.gen, None, None).await,
                ));
            }
            let stream = MergeInnerStream::<K, V>::new(streams)
                .await
                .map_err(CompactionError::Stream)?;

            let mut buf = Cursor::new(vec![0; 128]);
            let mut stream = pin!(stream);
            let mut key_builder = GenericBinaryBuilder::<Offset>::new();
            let mut value_builder = GenericBinaryBuilder::<Offset>::new();
            let mut written_size = 0;
            let mut min = None;
            let mut max = None;

            while let Some(result) = stream.next().await {
                let (key, value) = result.map_err(CompactionError::Stream)?;
                if min.is_none() {
                    min = Some(key.clone())
                }
                max = Some(key.clone());

                written_size += key.size();
                clear(&mut buf);
                key.encode(&mut buf)
                    .await
                    .map_err(CompactionError::KeyEncode)?;
                key_builder.append_value(buf.get_ref());

                if let Some(value) = value {
                    clear(&mut buf);
                    value
                        .encode(&mut buf)
                        .await
                        .map_err(CompactionError::ValueEncode)?;
                    value_builder.append_value(buf.get_ref());
                    written_size += value.size();
                } else {
                    value_builder.append_null();
                }

                if written_size >= option.max_sst_file_size {
                    Self::build_table(
                        option,
                        version_edits,
                        level,
                        &mut key_builder,
                        &mut value_builder,
                        &mut min,
                        &mut max,
                    )?;
                    written_size = 0;
                }
            }
            if written_size > 0 {
                Self::build_table(
                    option,
                    version_edits,
                    level,
                    &mut key_builder,
                    &mut value_builder,
                    &mut min,
                    &mut max,
                )?;
            }
            for scope in meet_scopes_l {
                version_edits.push(VersionEdit::Remove {
                    level: level as u8,
                    gen: scope.gen,
                })
            }
            for scope in meet_scopes_ll {
                version_edits.push(VersionEdit::Remove {
                    level: (level + 1) as u8,
                    gen: scope.gen,
                })
            }
            level += 1;
        }

        Ok(())
    }

    fn build_table(
        option: &DbOption,
        version_edits: &mut Vec<VersionEdit<K>>,
        level: usize,
        key_builder: &mut GenericByteBuilder<GenericBinaryType<Offset>>,
        value_builder: &mut GenericByteBuilder<GenericBinaryType<Offset>>,
        min: &mut Option<Arc<K>>,
        max: &mut Option<Arc<K>>,
    ) -> Result<(), CompactionError<K, V>> {
        assert!(min.is_some());
        assert!(max.is_some());

        let gen = ProcessUniqueId::new();
        let batch = RecordBatch::try_new(
            ELSM_SCHEMA.clone(),
            vec![
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )
        .map_err(CompactionError::Arrow)?;
        let mut writer = ArrowWriter::try_new(
            File::create(option.table_path(&gen)).map_err(CompactionError::Io)?,
            ELSM_SCHEMA.clone(),
            None,
        )
        .map_err(CompactionError::Parquet)?;
        writer.write(&batch).map_err(CompactionError::Parquet)?;
        writer.close().map_err(CompactionError::Parquet)?;
        version_edits.push(VersionEdit::Add {
            level: (level + 1) as u8,
            scope: Scope {
                min: min.take().unwrap(),
                max: max.take().unwrap(),
                gen,
            },
        });
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CompactionError<K, V>
where
    K: Encode + Decode,
    V: Encode + Decode,
{
    #[error("compaction key encode error: {0}")]
    KeyEncode(#[source] <K as Encode>::Error),
    #[error("compaction value encode error: {0}")]
    ValueEncode(#[source] <V as Encode>::Error),
    #[error("compaction io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("compaction arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("compaction parquet error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
    #[error("compaction version error: {0}")]
    Version(#[source] VersionError<K>),
    #[error("compaction stream error: {0}")]
    Stream(#[source] StreamError<K, V>),
}
