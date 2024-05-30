use std::{collections::VecDeque, error::Error, fmt::Debug, fs::File, sync::Arc};

use futures::channel::oneshot;
use parquet::arrow::ArrowWriter;
use snowflake::ProcessUniqueId;
use thiserror::Error;

use crate::{
    index_batch::IndexBatch, oracle::Oracle, scope::Scope, serdes::Encode, version::SyncVersion,
    DbOption, Immutable, ELSM_SCHEMA,
};
use crate::serdes::Decode;
use crate::version::apply_edits;
use crate::version::edit::VersionEdit;

pub(crate) struct Compactor<K, O>
where
    K: Ord + Encode + Decode + Debug,
    O: Oracle<K>,
{
    pub(crate) option: Arc<DbOption>,
    pub(crate) immutable: Immutable<K, O::Timestamp>,
    pub(crate) version: SyncVersion<K>,
}

impl<K, O> Compactor<K, O>
where
    K: Ord + Encode + Decode + Debug,
    O: Oracle<K>,
{
    pub(crate) fn new(
        immutable: Immutable<K, O::Timestamp>,
        option: Arc<DbOption>,
        version: SyncVersion<K>,
    ) -> Self {
        Compactor {
            option,
            immutable,
            version,
        }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        option_tx: Option<oneshot::Sender<()>>,
    ) -> Result<(), CompactionError<<K as Encode>::Error>> {
        let mut guard = self.immutable.write().await;

        if guard.len() > self.option.immutable_chunk_num {
            let excess = guard.split_off(self.option.immutable_chunk_num);

            if let Some(scope) = Self::minor_compaction(&self.option, excess).await? {
                let mut guard = self.version.write().await;

                apply_edits(&mut guard, vec![VersionEdit::Add { scope }], &self.option).await?;
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
    ) -> Result<Option<Scope<K>>, CompactionError<<K as Encode>::Error>> {
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
}

#[derive(Debug, Error)]
pub enum CompactionError<E: std::error::Error + Send + Sync> {
    #[error("compaction encode error: {0}")]
    Encode(#[from] E),
    #[error("compaction io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("compaction arrow error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
    #[error("compaction internal error: {0}")]
    Internal(#[source] Box<dyn Error + Send + Sync + 'static>),
}
