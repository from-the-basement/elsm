use std::{fs::OpenOptions, io::SeekFrom, sync::Arc};

use async_lock::RwLock;
use executor::{
    fs,
    futures::{util::SinkExt, AsyncSeekExt, AsyncWriteExt},
};
use futures::channel::mpsc::Sender;
use snowflake::ProcessUniqueId;

use crate::{
    serdes::{Decode, Encode},
    version::{cleaner::CleanTag, edit::VersionEdit, Version, VersionError, VersionRef},
    DbOption,
};

pub(crate) struct VersionSetInner<K>
where
    K: Encode + Decode + Ord,
{
    current: VersionRef<K>,
    log: fs::File,
}

pub(crate) struct VersionSet<K>
where
    K: Encode + Decode + Ord,
{
    inner: Arc<RwLock<VersionSetInner<K>>>,
    clean_sender: Sender<CleanTag>,
}

impl<K> Clone for VersionSet<K>
where
    K: Encode + Decode + Ord,
{
    fn clone(&self) -> Self {
        VersionSet {
            inner: self.inner.clone(),
            clean_sender: self.clean_sender.clone(),
        }
    }
}

impl<K> VersionSet<K>
where
    K: Encode + Decode + Ord,
{
    pub(crate) async fn new(
        option: &DbOption,
        clean_sender: Sender<CleanTag>,
    ) -> Result<Self, VersionError<K>> {
        let mut log = fs::File::from(
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(option.version_path())
                .map_err(VersionError::Io)?,
        );
        let edits = VersionEdit::recover(&mut log).await;
        log.seek(SeekFrom::End(0)).await.map_err(VersionError::Io)?;

        let set = VersionSet::<K> {
            inner: Arc::new(RwLock::new(VersionSetInner {
                current: Arc::new(Version {
                    num: 0,
                    level_slice: Version::level_slice_new(),
                    clean_sender: clean_sender.clone(),
                }),
                log,
            })),
            clean_sender,
        };
        set.apply_edits(edits, None, true).await?;

        Ok(set)
    }

    pub(crate) async fn current(&self) -> VersionRef<K> {
        self.inner.read().await.current.clone()
    }

    pub(crate) async fn apply_edits(
        &self,
        version_edits: Vec<VersionEdit<K>>,
        delete_gens: Option<Vec<ProcessUniqueId>>,
        is_recover: bool,
    ) -> Result<(), VersionError<K>> {
        let mut guard = self.inner.write().await;

        let mut new_version = Version::clone(&guard.current);

        for version_edit in version_edits {
            if !is_recover {
                version_edit
                    .encode(&mut guard.log)
                    .await
                    .map_err(VersionError::Encode)?;
            }
            match version_edit {
                VersionEdit::Add { scope, level } => {
                    new_version.level_slice[level as usize].push(scope);
                }
                VersionEdit::Remove { gen, level } => {
                    if let Some(i) = new_version.level_slice[level as usize]
                        .iter()
                        .position(|scope| scope.gen == gen)
                    {
                        new_version.level_slice[level as usize].remove(i);
                    }
                }
            }
        }
        if let Some(delete_gens) = delete_gens {
            new_version
                .clean_sender
                .send(CleanTag::Add {
                    version_num: new_version.num,
                    gens: delete_gens,
                })
                .await
                .map_err(VersionError::Send)?;
        }
        guard.log.flush().await.map_err(VersionError::Io)?;
        guard.current = Arc::new(new_version);
        Ok(())
    }
}
