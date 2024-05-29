use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fmt::Debug,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::StreamExt;
use futures::Stream;
use pin_project::pin_project;

use crate::{serdes::Decode, stream::EStreamImpl, utils::CmpKeyItem};

#[pin_project]
pub struct MergeStream<'stream, K, T, V, G, F>
where
    K: Ord,
    T: Ord + Copy + Default,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    #[allow(clippy::type_complexity)]
    heap: BinaryHeap<Reverse<(CmpKeyItem<Arc<K>, Option<G>>, usize)>>,
    iters: Vec<EStreamImpl<'stream, K, T, V, G, F>>,
    item_buf: Option<(Arc<K>, Option<G>)>,
}

impl<'stream, K, T, V, G, F> MergeStream<'stream, K, T, V, G, F>
where
    K: Ord + Debug,
    T: Ord + Copy + Default,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    pub(crate) async fn new(
        mut iters: Vec<EStreamImpl<'stream, K, T, V, G, F>>,
    ) -> Result<Self, V::Error> {
        let mut heap = BinaryHeap::new();

        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some(result) = Pin::new(iter).next().await {
                let (key, value) = result?;

                heap.push(Reverse((CmpKeyItem { key, _value: value }, i)));
            }
        }
        let mut iterator = MergeStream {
            iters,
            heap,
            item_buf: None,
        };

        {
            let mut iterator = pin!(&mut iterator);
            let _ = iterator.next().await;
        }

        Ok(iterator)
    }
}

impl<'stream, K, T, V, G, F> Stream for MergeStream<'stream, K, T, V, G, F>
where
    K: Ord + Debug,
    T: Ord + Copy + Default,
    V: Decode + Send + Sync,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<G>), V::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        while let Some(Reverse((
            CmpKeyItem {
                key: item_key,
                _value: item_value,
            },
            idx,
        ))) = this.heap.pop()
        {
            match Pin::new(&mut this.iters[idx]).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let (key, value) = item?;
                    this.heap
                        .push(Reverse((CmpKeyItem { key, _value: value }, idx)));

                    if let Some((buf_key, _)) = &this.item_buf {
                        if buf_key == &item_key {
                            continue;
                        }
                    }
                }
                Poll::Ready(None) => (),
                Poll::Pending => return Poll::Pending,
            };
            return Poll::Ready(this.item_buf.replace((item_key, item_value)).map(Ok));
        }
        Poll::Ready(this.item_buf.take().map(Ok))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::futures::StreamExt;
    use futures::executor::block_on;

    use crate::stream::{buf_stream::BufStream, merge_stream::MergeStream, EStreamImpl};

    #[test]
    fn iter() {
        block_on(async {
            let iter_1 = BufStream::new(vec![
                (Arc::new("key_1".to_owned()), Some("value_1".to_owned())),
                (Arc::new("key_3".to_owned()), None),
            ]);
            let iter_2 = BufStream::new(vec![
                (Arc::new("key_1".to_owned()), None),
                (Arc::new("key_2".to_owned()), Some("value_2".to_owned())),
                (Arc::new("key_4".to_owned()), None),
            ]);
            let iter_3 = BufStream::new(vec![
                (Arc::new("key_5".to_owned()), Some("value_3".to_owned())),
                (Arc::new("key_6".to_owned()), None),
            ]);

            let mut iterator =
                MergeStream::<String, u64, String, String, fn(&String) -> String>::new(vec![
                    EStreamImpl::Buf(iter_3),
                    EStreamImpl::Buf(iter_2),
                    EStreamImpl::Buf(iter_1),
                ])
                .await
                .unwrap();

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (Arc::new("key_1".to_owned()), None)
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (Arc::new("key_2".to_owned()), Some("value_2".to_owned()))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (Arc::new("key_3".to_owned()), None)
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (Arc::new("key_4".to_owned()), None)
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (Arc::new("key_5".to_owned()), Some("value_3".to_owned()))
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (Arc::new("key_6".to_owned()), None)
            );
        });
    }
}
