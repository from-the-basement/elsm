use std::{borrow::Borrow, ops::Bound, pin::pin, sync::Arc};

use crossbeam_skiplist::SkipMap;
use futures::StreamExt;

use crate::{record::RecordType, wal::WalRecover};

#[derive(Debug)]
pub(crate) struct MemTable<K, V, T>
where
    K: Ord,
    T: Ord,
{
    data: SkipMap<Arc<K>, SkipMap<T, Option<V>>>,
}

impl<K, V, T> Default for MemTable<K, V, T>
where
    K: Ord,
    T: Ord,
{
    fn default() -> Self {
        Self {
            data: SkipMap::default(),
        }
    }
}

impl<K, V, T> MemTable<K, V, T>
where
    K: Ord + Send + 'static,
    T: Ord + Send + 'static,
    V: Send + 'static,
{
    pub(crate) async fn new<W>(wal: &W) -> Result<Self, W::Error>
    where
        W: WalRecover<Arc<K>, V, T>,
    {
        let this = Self {
            data: SkipMap::new(),
        };

        this.recover(wal).await?;

        Ok(this)
    }

    pub(crate) async fn recover<W>(&self, wal: &W) -> Result<(), W::Error>
    where
        W: WalRecover<Arc<K>, V, T>,
    {
        let mut stream = pin!(wal.recover());
        let mut batch = None;
        while let Some(record) = stream.next().await {
            let record = record?;
            match record.record_type {
                RecordType::Full => {
                    self.data
                        .get_or_insert_with(record.key, || SkipMap::new())
                        .value()
                        .insert(record.ts, record.value);
                }
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
                    if let Some(b) = &mut batch {
                        b.push(record);
                        let batch = batch.take().unwrap();
                        for record in batch {
                            self.data
                                .get_or_insert_with(record.key, || SkipMap::new())
                                .value()
                                .insert(record.ts, record.value);
                        }
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
    K: Ord + 'static,
    T: Ord + Send + 'static,
    V: Sync + Send + 'static,
{
    pub(crate) fn insert(&self, key: Arc<K>, ts: T, value: Option<V>) {
        self.data
            .get_or_insert_with(key, || SkipMap::new())
            .value()
            .insert(ts, value);
    }

    pub(crate) fn put_batch(&self, mut kvs: impl ExactSizeIterator<Item = (Arc<K>, T, Option<V>)>) {
        for (key, ts, value) in &mut kvs {
            self.insert(key, ts, value);
        }
    }

    pub(crate) fn get<G, Q>(&self, key: &Q, ts: &T, f: impl FnOnce(&V) -> G) -> Option<G>
    where
        Q: ?Sized + Ord,
        Arc<K>: Borrow<Q>,
    {
        if let Some(entry) = self.data.get(key) {
            if let Some(entry) = entry
                .value()
                .range((Bound::Unbounded, Bound::Included(ts)))
                .next_back()
            {
                return entry.value().as_ref().map(f);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{executor::block_on, io::Cursor};

    use super::MemTable;
    use crate::record::{Record, RecordType};
    use crate::wal::{WalFile, WalWrite};

    #[test]
    fn recover_from_wal() {
        let mut file = Vec::new();
        let key = Arc::new("key".to_owned());
        let value = "value".to_owned();
        block_on(async {
            {
                let wal = WalFile::new(Cursor::new(&mut file), crc32fast::Hasher::new);
                wal.write(Record::new(RecordType::Full, &key, &0, Some(&value)))
                    .await
                    .unwrap();
                wal.flush().await.unwrap();
            }
            dbg!(&file);
            {
                let wal = WalFile::new(Cursor::new(&mut file), crc32fast::Hasher::new);
                let mem_table: MemTable<String, String, u64> = MemTable::new(&wal).await.unwrap();
                assert_eq!(mem_table.get(&key, &0, |v: &String| v.clone()), Some(value));
            }
        });
    }
}
