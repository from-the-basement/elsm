use std::{
    collections::{btree_map::Range, BTreeMap, Bound},
    fmt::Debug,
    iter::Iterator,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::array::{AsArray, RecordBatch};
use executor::futures::{util::io::Cursor, FutureExt, Stream, StreamExt};

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

    pub(crate) async fn range<V, G, F>(
        &self,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
        ts: &T,
        f: F,
    ) -> Result<Pin<Box<IndexBatchIterator<K, T, V, G, F>>>, V::Error>
    where
        V: Decode + Sync + Send,
        G: Send + 'static,
        F: Fn(&V) -> G + Sync + 'static,
    {
        let mut iterator = Box::pin(IndexBatchIterator {
            batch: &self.batch,
            inner: self.index.range((
                lower
                    .map(|k| {
                        Bound::Included(InternalKey {
                            key: k.clone(),
                            ts: *ts,
                        })
                    })
                    .unwrap_or(Bound::Unbounded),
                upper
                    .map(|k| {
                        Bound::Included(InternalKey {
                            key: k.clone(),
                            ts: T::default(),
                        })
                    })
                    .unwrap_or(Bound::Unbounded),
            )),
            item_buf: None,
            ts: *ts,
            f,
            _p: Default::default(),
        });
        // filling first item
        let _ = iterator.next().await;

        Ok(iterator)
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

impl<'a, K, T, V, G, F> Stream for IndexBatchIterator<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode + Send + Sync,
    G: Send + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = Result<(&'a Arc<K>, Option<G>), V::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        for (InternalKey { key, ts }, offset) in self.inner.by_ref() {
            if ts <= &self.ts
                && matches!(
                    self.item_buf.as_ref().map(|(k, _)| *k != key),
                    Some(true) | None
                )
            {
                let mut future =
                    Box::pin(IndexBatch::<K, T>::decode_value::<V>(self.batch, *offset));

                return match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(option)) => Poll::Ready(
                        self.item_buf
                            .replace((key, option.map(|v| (self.f)(&v))))
                            .map(Ok),
                    ),
                    Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                    Poll::Pending => Poll::Pending,
                };
            }
        }
        Poll::Ready(self.item_buf.take().map(Ok))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::{futures::StreamExt, ExecutorBuilder};
    use futures::executor::block_on;

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
                .range(Some(&key_1), Some(&key_2), &1, |v: &String| v.clone())
                .await
                .unwrap();

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (&Arc::new("key_1".to_owned()), None)
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (&Arc::new("key_2".to_owned()), Some(value_2))
            );
            assert!(iterator.next().await.is_none())
        })
    }
}
