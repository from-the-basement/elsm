use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::StreamExt;
use futures::Stream;

use crate::{serdes::Decode, utils::CmpKeyItem};

pub struct MergeIterator<'a, K, V, G>
where
    K: Ord,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
{
    #[allow(clippy::type_complexity)]
    heap: BinaryHeap<Reverse<(CmpKeyItem<&'a Arc<K>, Option<G>>, usize)>>,
    iters: Vec<Pin<Box<dyn Stream<Item = Result<(&'a Arc<K>, Option<G>), V::Error>>>>>,
    item_buf: Option<(&'a Arc<K>, Option<G>)>,
}

impl<'a, K, V, G> MergeIterator<'a, K, V, G>
where
    K: Ord,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
{
    pub(crate) async fn new(
        mut iters: Vec<Pin<Box<dyn Stream<Item = Result<(&'a Arc<K>, Option<G>), V::Error>>>>>,
    ) -> Result<Self, V::Error> {
        let mut heap = BinaryHeap::new();

        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some(result) = iter.next().await {
                let (key, value) = result?;

                heap.push(Reverse((CmpKeyItem { key, _value: value }, i)));
            }
        }
        let mut iterator = Box::pin(MergeIterator {
            iters,
            heap,
            item_buf: None,
        });
        let _ = iterator.next().await;

        unsafe {
            let raw: *mut MergeIterator<K, V, G> =
                Box::into_raw(Pin::into_inner_unchecked(iterator));

            Ok(*Box::from_raw(raw))
        }
    }
}

impl<'a, K, V, G> Stream for MergeIterator<'a, K, V, G>
where
    K: Ord,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
{
    type Item = Result<(&'a Arc<K>, Option<G>), V::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some(Reverse((
            CmpKeyItem {
                key: item_key,
                _value: item_value,
            },
            idx,
        ))) = self.heap.pop()
        {
            match self.iters[idx].poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let (key, value) = item?;
                    self.heap
                        .push(Reverse((CmpKeyItem { key, _value: value }, idx)));

                    if let Some((buf_key, _)) = &self.item_buf {
                        if *buf_key == item_key {
                            continue;
                        }
                    }
                }
                Poll::Ready(None) => (),
                Poll::Pending => return Poll::Pending,
            };
            return Poll::Ready(self.item_buf.replace((item_key, item_value)).map(Ok));
        }
        Poll::Ready(self.item_buf.take().map(Ok))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::futures::StreamExt;
    use futures::executor::block_on;

    use crate::iterator::{buf_iterator::BufIterator, merge_iterator::MergeIterator};

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

            let mut iterator = MergeIterator::<String, String, String>::new(vec![
                Box::pin(iter_3),
                Box::pin(iter_2),
                Box::pin(iter_1),
            ])
            .await
            .unwrap();

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (&Arc::new("key_1".to_owned()), None)
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (&Arc::new("key_2".to_owned()), Some("value_2".to_owned()))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (&Arc::new("key_3".to_owned()), None)
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (&Arc::new("key_4".to_owned()), None)
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (&Arc::new("key_5".to_owned()), Some("value_3".to_owned()))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (&Arc::new("key_6".to_owned()), None)
            );
        });
    }
}
