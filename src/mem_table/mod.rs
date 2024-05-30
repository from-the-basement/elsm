pub(crate) mod stream;

use std::{cmp, cmp::Ordering, collections::BTreeMap, ops::Bound, pin::pin, sync::Arc};

use futures::StreamExt;

use crate::{
    record::RecordType,
    serdes::{Decode, Encode},
    wal::WalRecover,
};

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct InternalKey<K, T> {
    pub(crate) key: Arc<K>,
    pub(crate) ts: T,
}

impl<K, T> PartialOrd<Self> for InternalKey<K, T>
where
    K: Ord,
    T: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, T> Ord for InternalKey<K, T>
where
    K: Ord,
    T: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| other.ts.cmp(&self.ts))
    }
}

#[derive(Debug)]
pub(crate) struct MemTable<K, V, T>
where
    K: Ord,
    T: Ord,
{
    pub(crate) data: BTreeMap<InternalKey<K, T>, Option<V>>,
    max_ts: T,
    written_size: usize,
}

impl<K, V, T> Default for MemTable<K, V, T>
where
    K: Ord,
    T: Ord + Default,
{
    fn default() -> Self {
        Self {
            data: BTreeMap::default(),
            max_ts: T::default(),
            written_size: 0,
        }
    }
}

impl<K, V, T> MemTable<K, V, T>
where
    K: Encode + Ord,
    T: Encode + Ord + Copy + Default,
    V: Encode + Decode,
{
    pub(crate) async fn from_wal<W>(wal: &mut W) -> Result<Self, W::Error>
    where
        W: WalRecover<Arc<K>, V, T>,
    {
        let mut mem_table = Self::default();

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
    K: Encode + Ord,
    T: Encode + Ord + Copy + Default,
    V: Encode + Decode,
{
    pub(crate) fn is_excess(&self, max_size: usize) -> bool {
        self.written_size > max_size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn insert(&mut self, key: Arc<K>, ts: T, value: Option<V>) {
        self.max_ts = cmp::max(self.max_ts, ts);
        self.written_size = key.size() + ts.size() + value.as_ref().map(Encode::size).unwrap_or(0);

        let _ = self.data.insert(InternalKey { key, ts }, value);
    }

    pub(crate) fn get(&self, key: &Arc<K>, ts: &T) -> Option<Option<&V>> {
        let internal_key = InternalKey {
            key: key.clone(),
            ts: *ts,
        };

        self.data
            .range((Bound::Included(&internal_key), Bound::Unbounded))
            .next()
            .and_then(|(InternalKey { key: item_key, .. }, value)| {
                (item_key == key).then_some(value.as_ref())
            })
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
    fn crud() {
        block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let key_3 = Arc::new("key_3".to_owned());
            let key_4 = Arc::new("key_4".to_owned());
            let value_1 = "value_1".to_owned();
            let value_3 = "value_3".to_owned();

            let mut mem_table = MemTable::default();

            mem_table.insert(key_1.clone(), 0, Some(value_1.clone()));
            mem_table.insert(key_1.clone(), 1, Some(value_1.clone()));
            mem_table.insert(key_1.clone(), 2, Some(value_1.clone()));

            mem_table.insert(key_3.clone(), 0, Some(value_3.clone()));

            assert_eq!(mem_table.get(&key_1, &0), Some(Some(&value_1)));
            assert_eq!(mem_table.get(&key_1, &1), Some(Some(&value_1)));
            assert_eq!(mem_table.get(&key_1, &2), Some(Some(&value_1)));

            assert_eq!(mem_table.get(&key_3, &0), Some(Some(&value_3)));

            assert_eq!(mem_table.get(&key_2, &0), None);
            assert_eq!(mem_table.get(&key_4, &0), None);
            assert_eq!(mem_table.get(&key_1, &3), Some(Some(&value_1)));
        });
    }

    #[test]
    fn recover_from_wal() {
        let mut file = Vec::new();
        let key = Arc::new("key".to_owned());
        let value = "value".to_owned();
        block_on(async {
            {
                let mut wal = WalFile::new(Cursor::new(&mut file));
                wal.write(Record::new(RecordType::Full, &key, &0, Some(&value)))
                    .await
                    .unwrap();
                wal.flush().await.unwrap();
            }
            {
                let mut wal = WalFile::new(Cursor::new(&mut file));
                let mem_table: MemTable<String, String, u64> =
                    MemTable::from_wal(&mut wal).await.unwrap();
                assert_eq!(mem_table.get(&key, &0), Some(Some(&value)));
            }
        });
    }
}
