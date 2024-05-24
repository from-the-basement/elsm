use std::{
    collections::{btree_map, Bound},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::{util::StreamExt, Stream};

use crate::{
    mem_table::{InternalKey, MemTable},
    serdes::Decode,
};

pub(crate) struct MemTableStream<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    inner: btree_map::Range<'a, InternalKey<K, T>, Option<V>>,
    item_buf: Option<(Arc<K>, Option<G>)>,
    ts: T,
    f: F,
}

impl<'a, K, V, T, G, F> Stream for MemTableStream<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<G>), V::Error>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        for (InternalKey { key, ts }, value) in this.inner.by_ref() {
            if ts <= &this.ts
                && matches!(
                    this.item_buf.as_ref().map(|(k, _)| k != key),
                    Some(true) | None
                )
            {
                return Poll::Ready(
                    this.item_buf
                        .replace((key.clone(), value.as_ref().map(|v| (this.f)(v))))
                        .map(Ok),
                );
            }
        }
        Poll::Ready(this.item_buf.take().map(Ok))
    }
}

impl<K, V, T> MemTable<K, V, T>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
{
    pub(crate) async fn iter<G, F>(&self, f: F) -> Result<MemTableStream<K, T, V, G, F>, V::Error>
    where
        G: Send + Sync + 'static,
        F: Fn(&V) -> G + Sync + 'static,
    {
        let mut iterator = Box::pin(MemTableStream {
            inner: self
                .data
                .range::<InternalKey<K, T>, (Bound<InternalKey<K, T>>, Bound<InternalKey<K, T>>)>(
                    (Bound::Unbounded, Bound::Unbounded),
                ),
            item_buf: None,
            ts: self.max_ts,
            f,
        });
        // filling first item
        let _ = iterator.next().await;

        unsafe {
            let raw: *mut MemTableStream<K, T, V, G, F> =
                Box::into_raw(Pin::into_inner_unchecked(iterator));

            Ok(*Box::from_raw(raw))
        }
    }

    pub(crate) async fn range<G, F>(
        &self,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
        ts: &T,
        f: F,
    ) -> Result<MemTableStream<K, T, V, G, F>, V::Error>
    where
        G: Send + Sync + 'static,
        F: Fn(&V) -> G + Sync + 'static,
    {
        let mut iterator = Box::pin(MemTableStream {
            inner: self.data.range((
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
        });
        // filling first item
        let _ = iterator.next().await;

        unsafe {
            let raw: *mut MemTableStream<K, T, V, G, F> =
                Box::into_raw(Pin::into_inner_unchecked(iterator));

            Ok(*Box::from_raw(raw))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::futures::{future::block_on, StreamExt};

    use crate::mem_table::MemTable;

    #[test]
    fn iterator() {
        block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let value_1 = "value_1".to_owned();
            let value_2 = "value_2".to_owned();

            let mut mem_table = MemTable::default();

            mem_table.insert(key_1.clone(), 0, Some(value_1.clone()));
            mem_table.insert(key_1.clone(), 1, Some(value_2.clone()));

            mem_table.insert(key_2.clone(), 0, Some(value_1.clone()));

            let mut iterator = mem_table.iter(|v| v.clone()).await.unwrap();

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (key_1.clone(), Some(value_2))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (key_2.clone(), Some(value_1))
            );

            drop(iterator);
            mem_table.insert(key_1.clone(), 3, None);

            let mut iterator = mem_table.iter(|v| v.clone()).await.unwrap();

            assert_eq!(iterator.next().await.unwrap().unwrap(), (key_1, None));
        });
    }

    #[test]
    fn range() {
        futures::executor::block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let key_3 = Arc::new("key_3".to_owned());
            let key_4 = Arc::new("key_4".to_owned());
            let value_1 = "value_1".to_owned();
            let value_2 = "value_2".to_owned();
            let value_3 = "value_3".to_owned();
            let value_4 = "value_4".to_owned();

            let mut mem_table = MemTable::default();

            mem_table.insert(key_1.clone(), 0, Some(value_1.clone()));
            mem_table.insert(key_2.clone(), 0, Some(value_2.clone()));
            mem_table.insert(key_2.clone(), 1, Some(value_3.clone()));
            mem_table.insert(key_3.clone(), 0, Some(value_3.clone()));
            mem_table.insert(key_4.clone(), 0, Some(value_4.clone()));

            let mut iterator = mem_table.iter(|v| v.clone()).await.unwrap();

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (key_1, Some(value_1.clone()))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (key_2.clone(), Some(value_3.clone()))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (key_3.clone(), Some(value_3.clone()))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (key_4, Some(value_4.clone()))
            );
            assert!(iterator.next().await.is_none());

            let mut iterator = mem_table
                .range(Some(&key_2), Some(&key_3), &0, |v| v.clone())
                .await
                .unwrap();

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (key_2, Some(value_2))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (key_3, Some(value_3))
            );
            assert!(iterator.next().await.is_none())
        });
    }
}