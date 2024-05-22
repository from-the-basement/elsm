use std::{
    io,
    marker::PhantomData,
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::Stream;

use crate::EIterator;

unsafe impl<K, V, E> Send for BufIterator<'_, K, V, E>
where
    K: Ord + Sync,
    V: Sync,
    E: Sync,
{
}
unsafe impl<K, V, E> Sync for BufIterator<'_, K, V, E>
where
    K: Ord + Sync,
    V: Sync,
    E: Sync,
{
}

pub(crate) struct BufIterator<'a, K, V, E>
where
    K: Ord,
{
    inner: NonNull<Vec<(Arc<K>, Option<V>)>>,
    pos: usize,
    _p: PhantomData<&'a E>,
}

impl<'a, K, V, E> BufIterator<'a, K, V, E>
where
    K: Ord,
    V: 'a,
{
    pub(crate) fn new(items: Vec<(Arc<K>, Option<V>)>) -> Self {
        BufIterator {
            inner: Box::leak(Box::new(items)).into(),
            pos: 0,
            _p: Default::default(),
        }
    }

    unsafe fn inner(&self) -> &'a [(Arc<K>, Option<V>)] {
        self.inner.as_ref()
    }

    unsafe fn take_item(&mut self) -> (&'a Arc<K>, Option<V>) {
        let value = self.inner.as_mut()[self.pos].1.take();
        let key = &self.inner.as_ref()[self.pos].0;

        (key, value)
    }
}

impl<'a, K, V, E> EIterator<K, E> for BufIterator<'a, K, V, E>
where
    K: Ord + 'a,
    V: 'a,
    E: From<io::Error> + std::error::Error + Send + Sync + 'static,
{
    type Item = (&'a Arc<K>, Option<V>);

    async fn try_next(&mut self) -> Result<Option<Self::Item>, E> {
        Ok(unsafe { self.pos < self.inner().len() }.then(|| {
            let result = unsafe { self.take_item() };
            self.pos += 1;
            result
        }))
    }
}

impl<'a, K, V, E> Stream for BufIterator<'a, K, V, E>
where
    K: Ord + 'a,
    V: 'a,
    E: From<io::Error> + std::error::Error + Send + Sync + 'static,
{
    type Item = Result<(&'a Arc<K>, Option<V>), E>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(unsafe { self.pos < self.inner().len() }.then(|| {
            let result = unsafe { self.take_item() };
            self.pos += 1;
            Ok(result)
        }))
    }
}

impl<K, V, E> Drop for BufIterator<'_, K, V, E>
where
    K: Ord,
{
    fn drop(&mut self) {
        unsafe { drop(Box::from_raw(self.inner.as_ptr())) }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, sync::Arc};

    use futures::executor::block_on;

    use crate::{iterator::buf_iterator::BufIterator, EIterator};

    #[test]
    fn iter() {
        block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let value_1 = "value_1".to_owned();

            let mut iter: BufIterator<String, String, io::Error> = BufIterator::new(vec![
                (key_1.clone(), Some(value_1.clone())),
                (key_2.clone(), None),
            ]);

            assert_eq!(
                iter.try_next().await.unwrap(),
                Some((&key_1, Some(value_1)))
            );
            assert_eq!(iter.try_next().await.unwrap(), Some((&key_2, None)));
            assert_eq!(iter.try_next().await.unwrap(), None);
        });
    }
}
