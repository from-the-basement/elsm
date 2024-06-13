use std::{
    cmp, collections::VecDeque, fmt::Debug, fs::File, marker::PhantomData, pin::pin, sync::Arc,
};

use arrow::{
    array::{GenericBinaryBuilder, GenericByteBuilder, RecordBatch},
    datatypes::GenericBinaryType,
};
use executor::{
    fs,
    futures::{util::io::Cursor, StreamExt},
};
use futures::channel::oneshot;
use parquet::arrow::{ArrowWriter, AsyncArrowWriter};
use snowflake::ProcessUniqueId;
use thiserror::Error;

use crate::{
    index_batch::IndexBatch,
    oracle::Oracle,
    scope::Scope,
    serdes::{Decode, Encode},
    stream::{
        level_stream::LevelStream, merge_inner_stream::MergeInnerStream, table_stream::TableStream,
        EInnerStreamImpl, StreamError,
    },
    version::{edit::VersionEdit, set::VersionSet, Version, VersionError, MAX_LEVEL},
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
    pub(crate) version_set: VersionSet<K>,
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
        version_set: VersionSet<K>,
    ) -> Self {
        Compactor::<K, O, V> {
            option,
            immutable,
            version_set,
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
                let version_ref = self.version_set.current().await;
                let mut version_edits = vec![];
                let mut delete_gens = vec![];

                if self.option.is_threshold_exceeded_major(&version_ref, 0) {
                    Self::major_compaction(
                        &version_ref,
                        &self.option,
                        &scope.min,
                        &scope.max,
                        &mut version_edits,
                        &mut delete_gens,
                    )
                    .await?;
                }
                version_edits.insert(0, VersionEdit::Add { level: 0, scope });

                self.version_set
                    .apply_edits(version_edits, Some(delete_gens), false)
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

            let mut writer = AsyncArrowWriter::try_new(
                fs::File::from(File::create(option.table_path(&gen)).map_err(CompactionError::Io)?),
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
                    .await
                    .map_err(CompactionError::Parquet)?;
            }
            writer.close().await.map_err(CompactionError::Parquet)?;
            return Ok(Some(Scope {
                min: min.ok_or(CompactionError::EmptyLevel)?,
                max: max.ok_or(CompactionError::EmptyLevel)?,
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
        delete_gens: &mut Vec<ProcessUniqueId>,
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
                    let min_key = &meet_scopes_l
                        .first()
                        .ok_or(CompactionError::EmptyLevel)?
                        .min;
                    let max_key = &meet_scopes_l.last().ok_or(CompactionError::EmptyLevel)?.max;
                    min = min_key;
                    max = max_key;

                    let min_index =
                        Version::<K>::scope_search(min_key, &version.level_slice[level + 1]);
                    let max_index =
                        Version::<K>::scope_search(max_key, &version.level_slice[level + 1]);

                    let next_level_len = version.level_slice[level + 1].len();
                    for scope in version.level_slice[level + 1]
                        [min_index..cmp::min(max_index + 1, next_level_len - 1)]
                        .iter()
                    {
                        if scope.is_between(min) || scope.is_between(max) {
                            meet_scopes_ll.push(scope);
                        }
                    }
                }
            }
            let mut streams = Vec::with_capacity(meet_scopes_l.len() + meet_scopes_ll.len());

            // This Level
            if level == 0 {
                for scope in meet_scopes_l.iter() {
                    streams.push(EInnerStreamImpl::Table(
                        TableStream::new(option, &scope.gen, None, None)
                            .await
                            .map_err(CompactionError::Stream)?,
                    ));
                }
            } else {
                let gens = meet_scopes_l
                    .iter()
                    .map(|scope| scope.gen)
                    .collect::<Vec<_>>();
                streams.push(EInnerStreamImpl::Level(
                    LevelStream::new(option, gens, Some(min), Some(max))
                        .await
                        .map_err(CompactionError::Stream)?,
                ));
            }
            // Next Level
            let gens = meet_scopes_ll
                .iter()
                .map(|scope| scope.gen)
                .collect::<Vec<_>>();
            streams.push(EInnerStreamImpl::Level(
                LevelStream::new(option, gens, None, None)
                    .await
                    .map_err(CompactionError::Stream)?,
            ));
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
                });
                delete_gens.push(scope.gen);
            }
            for scope in meet_scopes_ll {
                version_edits.push(VersionEdit::Remove {
                    level: (level + 1) as u8,
                    gen: scope.gen,
                });
                delete_gens.push(scope.gen);
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
                min: min.take().ok_or(CompactionError::EmptyLevel)?,
                max: max.take().ok_or(CompactionError::EmptyLevel)?,
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
    #[error("the level being compacted does not have a table")]
    EmptyLevel,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        fs::File,
        sync::Arc,
    };

    use arrow::array::{GenericBinaryBuilder, RecordBatch};
    use executor::{futures::util::io::Cursor, ExecutorBuilder};
    use futures::channel::mpsc::channel;
    use parquet::arrow::ArrowWriter;
    use snowflake::ProcessUniqueId;
    use tempfile::TempDir;

    use crate::{
        compactor::Compactor,
        index_batch::IndexBatch,
        mem_table::InternalKey,
        oracle::LocalOracle,
        scope::Scope,
        serdes::Encode,
        version::{edit::VersionEdit, Version},
        DbOption, Offset, ELSM_SCHEMA,
    };

    async fn build_index_batch<K, V>(items: &[(Arc<K>, Option<V>)]) -> IndexBatch<K, u64>
    where
        K: Encode + Ord,
        V: Encode,
    {
        fn clear(buf: &mut Cursor<Vec<u8>>) {
            buf.get_mut().clear();
            buf.set_position(0);
        }

        let mut buf = Cursor::new(vec![0; 128]);
        let mut index = BTreeMap::new();
        let mut key_builder = GenericBinaryBuilder::<Offset>::new();
        let mut value_builder = GenericBinaryBuilder::<Offset>::new();

        for (offset, (key, value)) in items.into_iter().enumerate() {
            clear(&mut buf);
            key.encode(&mut buf).await.unwrap();
            key_builder.append_value(buf.get_ref());

            if let Some(value) = value {
                clear(&mut buf);
                value.encode(&mut buf).await.unwrap();
                value_builder.append_value(buf.get_ref());
            } else {
                value_builder.append_null();
            }
            index.insert(
                InternalKey {
                    key: key.clone(),
                    ts: 0,
                },
                offset as u32,
            );
        }
        let keys = key_builder.finish();
        let values = value_builder.finish();

        let batch =
            RecordBatch::try_new(ELSM_SCHEMA.clone(), vec![Arc::new(keys), Arc::new(values)])
                .unwrap();

        IndexBatch { batch, index }
    }

    async fn build_parquet_table<K: Encode, V: Encode>(
        option: &DbOption,
        gen: ProcessUniqueId,
        items: &[(Arc<K>, Option<V>)],
    ) {
        fn clear(buf: &mut Cursor<Vec<u8>>) {
            buf.get_mut().clear();
            buf.set_position(0);
        }

        let mut buf = Cursor::new(vec![0; 128]);
        let mut key_builder = GenericBinaryBuilder::<Offset>::new();
        let mut value_builder = GenericBinaryBuilder::<Offset>::new();

        for (key, value) in items.iter() {
            clear(&mut buf);
            key.encode(&mut buf).await.unwrap();
            key_builder.append_value(buf.get_ref());

            if let Some(value) = value {
                clear(&mut buf);
                value.encode(&mut buf).await.unwrap();
                value_builder.append_value(buf.get_ref());
            } else {
                value_builder.append_null();
            }
        }
        let keys = key_builder.finish();
        let values = value_builder.finish();

        let batch =
            RecordBatch::try_new(ELSM_SCHEMA.clone(), vec![Arc::new(keys), Arc::new(values)])
                .unwrap();

        let mut writer = ArrowWriter::try_new(
            File::create(option.table_path(&gen)).unwrap(),
            ELSM_SCHEMA.clone(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn minor_compaction() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let option = DbOption::new(temp_dir.path().to_path_buf());
            let batch_1 = build_index_batch::<String, String>(&vec![
                (Arc::new("key_3".to_string()), Some("value_3".to_string())),
                (Arc::new("key_5".to_string()), Some("value_5".to_string())),
                (Arc::new("key_6".to_string()), Some("value_6".to_string())),
            ])
            .await;
            let batch_2 = build_index_batch::<String, String>(&vec![
                (Arc::new("key_4".to_string()), Some("value_4".to_string())),
                (Arc::new("key_2".to_string()), Some("value_2".to_string())),
                (Arc::new("key_1".to_string()), Some("value_1".to_string())),
            ])
            .await;

            let scope = Compactor::<String, LocalOracle<String>, String>::minor_compaction(
                &option,
                VecDeque::from(vec![batch_2, batch_1]),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(scope.min, Arc::new("key_1".to_string()));
            assert_eq!(scope.max, Arc::new("key_6".to_string()));
        })
    }

    #[test]
    fn major_compaction() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let mut option = DbOption::new(temp_dir.path().to_path_buf());
            option.major_threshold_with_sst_size = 2;

            // level 1
            let table_gen_1 = ProcessUniqueId::new();
            let table_gen_2 = ProcessUniqueId::new();
            build_parquet_table(
                &option,
                table_gen_1,
                &vec![
                    (Arc::new("key_1".to_string()), Some("value_1".to_string())),
                    (Arc::new("key_2".to_string()), Some("value_2".to_string())),
                    (Arc::new("key_3".to_string()), Some("value_3".to_string())),
                ],
            )
            .await;
            build_parquet_table(
                &option,
                table_gen_2,
                &vec![
                    (Arc::new("key_4".to_string()), Some("value_4".to_string())),
                    (Arc::new("key_5".to_string()), Some("value_5".to_string())),
                    (Arc::new("key_6".to_string()), Some("value_6".to_string())),
                ],
            )
            .await;

            // level 2
            let table_gen_3 = ProcessUniqueId::new();
            let table_gen_4 = ProcessUniqueId::new();
            let table_gen_5 = ProcessUniqueId::new();
            build_parquet_table(
                &option,
                table_gen_3,
                &vec![
                    (Arc::new("key_1".to_string()), Some("value_1".to_string())),
                    (Arc::new("key_2".to_string()), Some("value_2".to_string())),
                    (Arc::new("key_3".to_string()), Some("value_3".to_string())),
                ],
            )
            .await;
            build_parquet_table(
                &option,
                table_gen_4,
                &vec![
                    (Arc::new("key_4".to_string()), Some("value_4".to_string())),
                    (Arc::new("key_5".to_string()), Some("value_5".to_string())),
                    (Arc::new("key_6".to_string()), Some("value_6".to_string())),
                ],
            )
            .await;
            build_parquet_table(
                &option,
                table_gen_5,
                &vec![
                    (Arc::new("key_7".to_string()), Some("value_7".to_string())),
                    (Arc::new("key_8".to_string()), Some("value_8".to_string())),
                    (Arc::new("key_9".to_string()), Some("value_9".to_string())),
                ],
            )
            .await;

            let (sender, _) = channel(1);

            let mut version = Version {
                num: 0,
                level_slice: Version::level_slice_new(),
                clean_sender: sender,
            };
            version.level_slice[0].push(Scope {
                min: Arc::new("key_1".to_string()),
                max: Arc::new("key_3".to_string()),
                gen: table_gen_1,
            });
            version.level_slice[0].push(Scope {
                min: Arc::new("key_4".to_string()),
                max: Arc::new("key_6".to_string()),
                gen: table_gen_2,
            });
            version.level_slice[1].push(Scope {
                min: Arc::new("key_1".to_string()),
                max: Arc::new("key_3".to_string()),
                gen: table_gen_3,
            });
            version.level_slice[1].push(Scope {
                min: Arc::new("key_4".to_string()),
                max: Arc::new("key_6".to_string()),
                gen: table_gen_4,
            });
            version.level_slice[1].push(Scope {
                min: Arc::new("key_7".to_string()),
                max: Arc::new("key_9".to_string()),
                gen: table_gen_5,
            });

            let min = Arc::new("key_2".to_string());
            let max = Arc::new("key_5".to_string());
            let mut version_edits = Vec::new();

            Compactor::<String, LocalOracle<String>, String>::major_compaction(
                &version,
                &option,
                &min,
                &max,
                &mut version_edits,
                &mut vec![],
            )
            .await
            .unwrap();

            if let VersionEdit::Add { level, scope } = &version_edits[0] {
                assert_eq!(*level, 1);
                assert_eq!(scope.min, Arc::new("key_1".to_string()));
                assert_eq!(scope.max, Arc::new("key_6".to_string()));
            }
            assert_eq!(
                version_edits[1..5].to_vec(),
                vec![
                    VersionEdit::Remove {
                        level: 0,
                        gen: table_gen_1,
                    },
                    VersionEdit::Remove {
                        level: 0,
                        gen: table_gen_2,
                    },
                    VersionEdit::Remove {
                        level: 1,
                        gen: table_gen_3,
                    },
                    VersionEdit::Remove {
                        level: 1,
                        gen: table_gen_4,
                    },
                ]
            )
        })
    }
}
