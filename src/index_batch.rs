use std::{
    collections::{btree_map::Range, BTreeMap, Bound},
    fmt::Debug,
    iter::Iterator,
    marker::PhantomData,
    sync::Arc,
};

use arrow::array::{AsArray, RecordBatch};
use executor::futures::util::io::Cursor;

use crate::{mem_table::InternalKey, serdes::Decode, EIterator, Offset};

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
        V: Decode,
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

    pub(crate) async fn range<V, G, F>(
        &self,
        lower: &Arc<K>,
        upper: &Arc<K>,
        ts: &T,
        f: F,
    ) -> Result<IndexBatchIterator<K, T, V, G, F>, V::Error>
    where
        V: Decode,
        G: Send + 'static,
        F: Fn(&V) -> G + Sync + 'static,
    {
        let mut iterator = IndexBatchIterator {
            batch: &self.batch,
            inner: self.index.range((
                Bound::Included(&InternalKey {
                    key: lower.clone(),
                    ts: *ts,
                }),
                Bound::Included(&InternalKey {
                    key: upper.clone(),
                    ts: T::default(),
                }),
            )),
            item_buf: None,
            ts: *ts,
            f,
            _p: Default::default(),
        };
        // filling first item
        let _ = iterator.try_next().await?;

        Ok(iterator)
    }

    async fn decode_value<V>(batch: &RecordBatch, offset: u32) -> Result<Option<V>, V::Error>
    where
        V: Decode,
    {
        let bytes = batch.column(1).as_binary::<Offset>().value(offset as usize);

        if bytes.is_empty() {
            return Ok(None);
        }
        let mut cursor = Cursor::new(bytes);

        Ok(Some(V::decode(&mut cursor).await?))
    }
}

#[derive(Debug)]
pub(crate) struct IndexBatchIterator<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    batch: &'a RecordBatch,
    item_buf: Option<(&'a Arc<K>, Option<G>)>,
    inner: Range<'a, InternalKey<K, T>, u32>,
    ts: T,
    f: F,
    _p: PhantomData<V>,
}

impl<'a, K, T, V, G, F> EIterator<K, V::Error> for IndexBatchIterator<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = (&'a Arc<K>, Option<G>);

    async fn try_next(&mut self) -> Result<Option<Self::Item>, V::Error> {
        for (InternalKey { key, ts }, offset) in self.inner.by_ref() {
            if ts <= &self.ts
                && matches!(
                    self.item_buf.as_ref().map(|(k, _)| *k != key),
                    Some(true) | None
                )
            {
                let g = IndexBatch::<K, T>::decode_value::<V>(self.batch, *offset)
                    .await?
                    .map(|v| (self.f)(&v));
                return Ok(self.item_buf.replace((key, g)));
            }
        }
        Ok(self.item_buf.take())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::ExecutorBuilder;
    use futures::executor::block_on;

    use crate::{
        mem_table::MemTable, oracle::LocalOracle, wal::provider::in_mem::InMemProvider, Db,
        EIterator,
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

    #[test]
    fn range() {
        block_on(async {
            let key_0 = Arc::new("key_0".to_owned());
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let key_3 = Arc::new("key_3".to_owned());
            let value_1 = "value_1".to_owned();
            let value_2 = "value_2".to_owned();

            let mut mem_table = MemTable::default();

            mem_table.insert(key_0.clone(), 0, None);
            mem_table.insert(key_1.clone(), 0, Some(value_1.clone()));
            mem_table.insert(key_1.clone(), 1, None);
            mem_table.insert(key_2.clone(), 0, Some(value_2.clone()));
            mem_table.insert(key_3.clone(), 0, None);

            let batch = Db::<String, String, LocalOracle<String>, InMemProvider>::freeze(mem_table)
                .await
                .unwrap();

            let mut iterator = batch
                .range(&key_1, &key_2, &1, |v: &String| v.clone())
                .await
                .unwrap();

            assert_eq!(
                iterator.try_next().await.unwrap(),
                Some((&Arc::new("key_1".to_owned()), None))
            );
            assert_eq!(
                iterator.try_next().await.unwrap(),
                Some((&Arc::new("key_2".to_owned()), Some(value_2)))
            );
            assert_eq!(iterator.try_next().await.unwrap(), None);
        })
    }
}
