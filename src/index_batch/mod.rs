pub(crate) mod stream;

use std::{
    collections::{BTreeMap, Bound},
    fmt::Debug,
    iter::Iterator,
    sync::Arc,
};

use arrow::array::{AsArray, RecordBatch};
use executor::futures::util::io::Cursor;

use crate::{mem_table::InternalKey, serdes::Decode, Offset};

#[derive(Debug)]
pub(crate) struct IndexBatch<K, T>
where
    K: Ord,
    T: Ord,
{
    pub(crate) batch: RecordBatch,
    pub(crate) index: BTreeMap<InternalKey<K, T>, u32>,
}

impl<K, T> IndexBatch<K, T>
where
    K: Ord,
    T: Ord + Copy + Default,
{
    pub(crate) async fn find<V>(&self, key: &Arc<K>, ts: &T) -> Result<Option<Option<V>>, V::Error>
    where
        V: Decode + Sync + Send,
    {
        let internal_key = InternalKey {
            key: key.clone(),
            ts: *ts,
        };
        if let Some((InternalKey { key: item_key, .. }, offset)) = self
            .index
            .range((Bound::Included(&internal_key), Bound::Unbounded))
            .next()
        {
            if item_key == key {
                return Ok(Some(
                    IndexBatch::<K, T>::decode_value::<V>(&self.batch, *offset).await?,
                ));
            }
        }
        Ok(None)
    }

    async fn decode_value<V>(batch: &RecordBatch, offset: u32) -> Result<Option<V>, V::Error>
    where
        V: Decode + Sync + Send,
    {
        let bytes = batch.column(1).as_binary::<Offset>().value(offset as usize);

        if bytes.is_empty() {
            return Ok(None);
        }
        let mut cursor = Cursor::new(bytes);

        Ok(Some(V::decode(&mut cursor).await?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::ExecutorBuilder;

    use crate::{
        mem_table::MemTable, oracle::LocalOracle, wal::provider::in_mem::InMemProvider, Db,
    };

    #[test]
    fn find() {
        ExecutorBuilder::new().build().unwrap().block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let key_3 = Arc::new("key_3".to_owned());
            let value_1 = "value_1".to_owned();
            let value_2 = "value_2".to_owned();

            let mut mem_table = MemTable::default();

            mem_table.insert(key_1.clone(), 0, Some(value_1.clone()));
            mem_table.insert(key_1.clone(), 1, None);
            mem_table.insert(key_2.clone(), 0, Some(value_2.clone()));
            mem_table.insert(key_3.clone(), 0, None);

            let batch = Db::<String, String, LocalOracle<String>, InMemProvider>::freeze(mem_table)
                .await
                .unwrap();

            assert_eq!(
                batch.find::<String>(&key_1, &0).await.unwrap(),
                Some(Some(value_1))
            );
            assert_eq!(batch.find::<String>(&key_1, &1).await.unwrap(), Some(None));

            assert_eq!(
                batch.find::<String>(&key_2, &0).await.unwrap(),
                Some(Some(value_2))
            );
            assert_eq!(batch.find::<String>(&key_3, &0).await.unwrap(), Some(None));
        });
    }
}