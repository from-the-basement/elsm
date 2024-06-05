use std::{
    marker::PhantomData,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use executor::futures::{FutureExt, Stream};
use pin_project::pin_project;

use crate::{index_batch::decode_value, serdes::Decode};

#[pin_project]
pub(crate) struct BatchStream<K, V>
where
    K: Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    pos: usize,
    inner: RecordBatch,
    _p: PhantomData<(K, V)>,
}

impl<K, V> BatchStream<K, V>
where
    K: Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    pub(crate) fn new(batch: RecordBatch) -> Self {
        BatchStream {
            pos: 0,
            inner: batch,
            _p: Default::default(),
        }
    }

    async fn decode_item(&mut self) -> Result<(Arc<K>, Option<V>), V::Error> {
        // FIXME: handle error
        let key = decode_value::<K>(&self.inner, 0, self.pos)
            .await
            .unwrap()
            .unwrap();
        let value = decode_value::<V>(&self.inner, 1, self.pos).await?;

        self.pos += 1;
        Ok((Arc::new(key), value))
    }
}

impl<K, V> Stream for BatchStream<K, V>
where
    K: Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<V>), V::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.pos < self.inner.num_rows() {
            let mut future = pin!(self.decode_item());

            return match future.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(Some(result)),
                Poll::Pending => Poll::Pending,
            };
        }
        Poll::Ready(None)
    }
}
