use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::StreamExt;
use futures::Stream;
use pin_project::pin_project;

use crate::{
    schema::Schema,
    stream::{EStreamImpl, StreamError},
    utils::CmpKeyItem,
};

#[pin_project]
pub struct MergeStream<'stream, S>
where
    S: Schema,
{
    #[allow(clippy::type_complexity)]
    heap: BinaryHeap<Reverse<(CmpKeyItem<Arc<S::PrimaryKey>, Option<S>>, usize)>>,
    iters: Vec<EStreamImpl<'stream, S>>,
    item_buf: Option<(Arc<S::PrimaryKey>, Option<S>)>,
}

impl<'stream, S> MergeStream<'stream, S>
where
    S: Schema,
{
    pub(crate) async fn new(
        mut iters: Vec<EStreamImpl<'stream, S>>,
    ) -> Result<Self, StreamError<S::PrimaryKey, S>> {
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

impl<'stream, S> Stream for MergeStream<'stream, S>
where
    S: Schema,
{
    type Item = Result<(Arc<S::PrimaryKey>, Option<S>), StreamError<S::PrimaryKey, S>>;

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

    use crate::{
        stream::{buf_stream::BufStream, merge_stream::MergeStream, EStreamImpl},
        tests::UserInner,
    };

    #[test]
    fn iter() {
        block_on(async {
            let iter_1 = BufStream::new(vec![
                (
                    Arc::new(1),
                    Some(UserInner::new(
                        1,
                        "1".to_string(),
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    )),
                ),
                (Arc::new(3), None),
            ]);
            let iter_2 = BufStream::new(vec![
                (Arc::new(1), None),
                (
                    Arc::new(2),
                    Some(UserInner::new(
                        2,
                        "2".to_string(),
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    )),
                ),
                (Arc::new(4), None),
            ]);
            let iter_3 = BufStream::new(vec![
                (
                    Arc::new(5),
                    Some(UserInner::new(
                        3,
                        "3".to_string(),
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    )),
                ),
                (Arc::new(6), None),
            ]);

            let mut iterator = MergeStream::<UserInner>::new(vec![
                EStreamImpl::Buf(iter_3),
                EStreamImpl::Buf(iter_2),
                EStreamImpl::Buf(iter_1),
            ])
            .await
            .unwrap();

            assert_eq!(iterator.next().await.unwrap().unwrap(), (Arc::new(1), None));
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(2),
                    Some(UserInner::new(
                        2,
                        "2".to_string(),
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0
                    ))
                )
            );
            assert_eq!(iterator.next().await.unwrap().unwrap(), (Arc::new(3), None));
            assert_eq!(iterator.next().await.unwrap().unwrap(), (Arc::new(4), None));
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(5),
                    Some(UserInner::new(
                        3,
                        "3".to_string(),
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0
                    ))
                )
            );
            assert_eq!(iterator.next().await.unwrap().unwrap(), (Arc::new(6), None));
        });
    }
}
