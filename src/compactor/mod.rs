use std::{
    collections::VecDeque,
    error::Error,
    fs::File,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{array::RecordBatchWriter, ipc::writer::StreamWriter};
use futures::channel::oneshot;
use thiserror::Error;

use crate::{index_batch::IndexBatch, oracle::Oracle, DbOption, Immutable, ELSM_SCHEMA};

pub(crate) struct Compactor<K, O>
where
    K: Ord,
    O: Oracle<K>,
{
    pub(crate) option: Arc<DbOption>,
    pub(crate) immutable: Immutable<K, O::Timestamp>,
}

impl<K, O> Compactor<K, O>
where
    K: Ord,
    O: Oracle<K>,
{
    pub(crate) fn new(immutable: Immutable<K, O::Timestamp>, option: Arc<DbOption>) -> Self {
        Compactor { option, immutable }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        option_tx: Option<oneshot::Sender<()>>,
    ) -> Result<(), CompactionError> {
        let mut guard = self.immutable.write().await;

        if guard.len() > self.option.immutable_chunk_num {
            let excess = guard.split_off(self.option.immutable_chunk_num);
            let gen = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            Self::minor_compaction(gen as i64, &self.option, excess).await?;
        }
        if let Some(tx) = option_tx {
            let _ = tx.send(());
        }
        Ok(())
    }

    pub(crate) async fn minor_compaction(
        gen: i64,
        option: &DbOption,
        batches: VecDeque<IndexBatch<K, O::Timestamp>>,
    ) -> Result<(), CompactionError> {
        if !batches.is_empty() {
            let mut writer = StreamWriter::try_new(
                File::create(option.path.join(format!("{}.parquet", gen)))
                    .map_err(CompactionError::Io)?,
                &ELSM_SCHEMA,
            )
            .map_err(CompactionError::Arrow)?;

            for batch in batches {
                writer.write(&batch.batch).map_err(CompactionError::Arrow)?;
            }
            writer.close().map_err(CompactionError::Arrow)?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("compaction io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("compaction arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("compaction internal error: {0}")]
    Internal(#[source] Box<dyn Error + Send + Sync + 'static>),
}
