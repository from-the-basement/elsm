use std::{marker::PhantomData, ptr::NonNull, sync::Arc};

use crate::{serdes::Decode, EIterator};

unsafe impl<K, V, G, F> Send for BufIterator<'_, K, V, G, F>
where
    K: Ord + Sync,
    V: Decode + Sync,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
}
unsafe impl<K, V, G, F> Sync for BufIterator<'_, K, V, G, F>
where
    K: Ord + Sync,
    V: Decode + Sync,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
}

struct BufIterator<'a, K, V, G, F>
where
    K: Ord,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    inner: NonNull<Vec<(Arc<K>, Option<V>)>>,
    pos: usize,
    f: F,
    _p: PhantomData<&'a ()>,
}

impl<'a, K, V, G, F> BufIterator<'a, K, V, G, F>
where
    K: Ord,
    V: Decode,
    G: Send + Sync + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    unsafe fn inner(&self) -> &'a [(Arc<K>, Option<V>)] {
        self.inner.as_ref()
    }
}

impl<'a, K, V, G, F> EIterator<K, V::Error> for BufIterator<'a, K, V, G, F>
where
    K: Ord + 'a,
    V: Decode + 'a,
    G: Send + 'static + Sync + Clone,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = (&'a Arc<K>, Option<G>);

    async fn try_next(&mut self) -> Result<Option<Self::Item>, V::Error> {
        Ok(unsafe { self.pos < self.inner().len() }.then(|| {
            let (k, v) = &unsafe { self.inner() }[self.pos];
            self.pos += 1;

            (k, v.as_ref().map(|v| (self.f)(v)))
        }))
    }
}

impl<K, V, G, F> Drop for BufIterator<'_, K, V, G, F>
where
    K: Ord,
    V: Decode,
    G: Send + 'static + Sync,
    F: Fn(&V) -> G + Sync + 'static,
{
    fn drop(&mut self) {
        unsafe { drop(Box::from_raw(self.inner.as_ptr())) }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::executor::block_on;

    use crate::{iterator::buf_iterator::BufIterator, EIterator};

    #[test]
    fn iter() {
        block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let value_1 = "value_1".to_owned();

            let mut iter = BufIterator {
                inner: Box::leak(Box::new(vec![
                    (key_1.clone(), Some(value_1.clone())),
                    (key_2.clone(), None),
                ]))
                .into(),
                pos: 0,
                f: |v: &String| v.clone(),
                _p: Default::default(),
            };

            assert_eq!(
                iter.try_next().await.unwrap(),
                Some((&key_1, Some(value_1)))
            );
            assert_eq!(iter.try_next().await.unwrap(), Some((&key_2, None)));
            assert_eq!(iter.try_next().await.unwrap(), None);
        });
    }
}
