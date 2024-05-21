use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use crate::{iterator::EIteratorImpl, serdes::Decode, utils::CmpKeyItem, EIterator};

pub(crate) struct MergeIterator<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    #[allow(clippy::type_complexity)]
    heap: BinaryHeap<Reverse<(CmpKeyItem<&'a Arc<K>, Option<G>>, usize)>>,
    iters: Vec<EIteratorImpl<'a, K, T, V, G, F>>,
    item_buf: Option<(&'a Arc<K>, Option<G>)>,
}

impl<'a, K, T, V, G, F> MergeIterator<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    pub(crate) async fn new(
        mut iters: Vec<EIteratorImpl<'a, K, T, V, G, F>>,
    ) -> Result<Self, V::Error> {
        let mut heap = BinaryHeap::new();

        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some((key, value)) = iter.try_next().await? {
                heap.push(Reverse((CmpKeyItem { key, _value: value }, i)));
            }
        }
        let mut iterator = MergeIterator {
            iters,
            heap,
            item_buf: None,
        };
        let _ = iterator.try_next().await?;

        Ok(iterator)
    }
}

impl<'a, K, T, V, G, F> EIterator<K, V::Error> for MergeIterator<'a, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = (&'a Arc<K>, Option<G>);

    async fn try_next(&mut self) -> Result<Option<Self::Item>, V::Error> {
        while let Some(Reverse((
            CmpKeyItem {
                key: item_key,
                _value: item_value,
            },
            idx,
        ))) = self.heap.pop()
        {
            if let Some((key, value)) = self.iters[idx].try_next().await? {
                self.heap
                    .push(Reverse((CmpKeyItem { key, _value: value }, idx)));

                if let Some((buf_key, _)) = &self.item_buf {
                    if *buf_key == item_key {
                        continue;
                    }
                }
            }
            return Ok(self.item_buf.replace((item_key, item_value)));
        }
        Ok(self.item_buf.take())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::executor::block_on;

    use crate::{
        iterator::{buf_iterator::BufIterator, merge_iterator::MergeIterator, EIteratorImpl},
        EIterator,
    };

    #[test]
    fn iter() {
        block_on(async {
            let iter_1 = BufIterator::new(vec![
                (Arc::new("key_1".to_owned()), Some("value_1".to_owned())),
                (Arc::new("key_3".to_owned()), None),
            ]);
            let iter_2 = BufIterator::new(vec![
                (Arc::new("key_1".to_owned()), None),
                (Arc::new("key_2".to_owned()), Some("value_2".to_owned())),
                (Arc::new("key_4".to_owned()), None),
            ]);
            let iter_3 = BufIterator::new(vec![
                (Arc::new("key_5".to_owned()), Some("value_3".to_owned())),
                (Arc::new("key_6".to_owned()), None),
            ]);

            let mut iterator =
                MergeIterator::<String, u64, String, String, fn(&String) -> String>::new(vec![
                    EIteratorImpl::<String, u64, String, String, _>::Buf(iter_3),
                    EIteratorImpl::<String, u64, String, String, _>::Buf(iter_2),
                    EIteratorImpl::<String, u64, String, String, _>::Buf(iter_1),
                ])
                .await
                .unwrap();

            assert_eq!(
                iterator.try_next().await.unwrap(),
                Some((&Arc::new("key_1".to_owned()), None))
            );
            assert_eq!(
                iterator.try_next().await.unwrap(),
                Some((&Arc::new("key_2".to_owned()), Some("value_2".to_owned())))
            );
            assert_eq!(
                iterator.try_next().await.unwrap(),
                Some((&Arc::new("key_3".to_owned()), None))
            );
            assert_eq!(
                iterator.try_next().await.unwrap(),
                Some((&Arc::new("key_4".to_owned()), None))
            );
            assert_eq!(
                iterator.try_next().await.unwrap(),
                Some((&Arc::new("key_5".to_owned()), Some("value_3".to_owned())))
            );
            assert_eq!(
                iterator.try_next().await.unwrap(),
                Some((&Arc::new("key_6".to_owned()), None))
            );
        });
    }
}
