use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap},
    ops::Bound,
    pin::pin,
    sync::Arc,
};

use futures::StreamExt;

use crate::{record::RecordType, wal::WalRecover};

#[derive(Debug)]
pub(crate) struct MemTable<K, V, T>
where
    K: Ord,
    T: Ord,
{
    data: BTreeMap<Arc<K>, BTreeMap<T, Option<V>>>,
}

impl<K, V, T> Default for MemTable<K, V, T>
where
    K: Ord,
    T: Ord,
{
    fn default() -> Self {
        Self {
            data: BTreeMap::default(),
        }
    }
}

impl<K, V, T> MemTable<K, V, T>
where
    K: Ord,
    T: Ord,
{
    pub(crate) async fn from_wal<W>(wal: &mut W) -> Result<Self, W::Error>
    where
        W: WalRecover<Arc<K>, V, T>,
    {
        let mut mem_table = Self {
            data: BTreeMap::new(),
        };

        mem_table.recover(wal).await?;

        Ok(mem_table)
    }

    pub(crate) async fn recover<W>(&mut self, wal: &mut W) -> Result<(), W::Error>
    where
        W: WalRecover<Arc<K>, V, T>,
    {
        let mut stream = pin!(wal.recover());
        let mut batch = None;
        while let Some(record) = stream.next().await {
            let record = record?;
            match record.record_type {
                RecordType::Full => self.insert(record.key, record.ts, record.value),
                RecordType::First => {
                    if batch.is_none() {
                        batch = Some(vec![record]);
                        continue;
                    }
                    panic!("batch should be committed before next first record");
                }
                RecordType::Middle => {
                    if let Some(batch) = &mut batch {
                        batch.push(record);
                        continue;
                    }
                    panic!("middle record should in a batch");
                }
                RecordType::Last => {
                    if let Some(b) = batch.take() {
                        for r in b {
                            self.insert(r.key, r.ts, r.value);
                        }
                        self.insert(record.key, record.ts, record.value);
                        continue;
                    }
                    panic!("last record should in a batch");
                }
            }
        }
        Ok(())
    }
}

impl<K, V, T> MemTable<K, V, T>
where
    K: Ord,
    T: Ord,
{
    pub(crate) fn insert(&mut self, key: Arc<K>, ts: T, value: Option<V>) {
        match self.data.entry(key) {
            Entry::Vacant(vacant) => {
                let versions = vacant.insert(BTreeMap::new());
                versions.insert(ts, value);
            }
            Entry::Occupied(mut occupied) => {
                occupied.get_mut().insert(ts, value);
            }
        }
    }

    pub(crate) fn get<Q>(&self, key: &Q, ts: &T) -> Option<&V>
    where
        Q: ?Sized + Ord,
        Arc<K>: Borrow<Q>,
    {
        self.data
            .get(key)
            .and_then(|versions| {
                versions
                    .range((Bound::Unbounded, Bound::Included(ts)))
                    .next_back()
            })
            .and_then(|(_, v)| v.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{executor::block_on, io::Cursor};

    use super::MemTable;
    use crate::{
        record::{Record, RecordType},
        wal::{WalFile, WalWrite},
    };

    #[test]
    fn recover_from_wal() {
        let mut file = Vec::new();
        let key = Arc::new("key".to_owned());
        let value = "value".to_owned();
        block_on(async {
            {
                let mut wal = WalFile::new(Cursor::new(&mut file), usize::MAX);
                wal.write(Record::new(RecordType::Full, &key, &0, Some(&value)))
                    .await
                    .unwrap();
                wal.flush().await.unwrap();
            }
            {
                let mut wal = WalFile::new(Cursor::new(&mut file), usize::MAX);
                let mem_table: MemTable<String, String, u64> =
                    MemTable::from_wal(&mut wal).await.unwrap();
                assert_eq!(mem_table.get(&key, &0), Some(&value));
            }
        });
    }
}
