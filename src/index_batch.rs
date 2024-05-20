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
    K: Ord + Debug,
    T: Ord + Copy + Default + Debug,
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
        lower: Bound<&Arc<K>>,
        upper: Bound<&Arc<K>>,
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
                lower.map(|k| InternalKey {
                    key: k.clone(),
                    ts: *ts,
                }),
                upper.map(|k| InternalKey {
                    key: k.clone(),
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
        // only 'Bound::Excluded' and higher ts will cause the first element to be repeated
        if let (Bound::Excluded(lower), Some((key, _))) = (lower, &iterator.item_buf) {
            if lower.as_ref() == *key {
                let _ = iterator.try_next().await?;
            }
        }
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
    item_buf: Option<(&'a K, Option<G>)>,
    inner: Range<'a, InternalKey<K, T>, u32>,
    ts: T,
    f: F,
    _p: PhantomData<V>,
}

impl<'a, K, T, V, G, F> EIterator<K, V::Error> for IndexBatchIterator<'a, K, T, V, G, F>
where
    K: Ord + Debug,
    T: Ord + Copy + Default + Debug,
    V: Decode,
    G: Send + 'static,
    F: Fn(&V) -> G + Sync + 'static,
{
    type Item = (&'a K, Option<G>);

    async fn try_next(&mut self) -> Result<Option<Self::Item>, V::Error> {
        for (InternalKey { key, ts }, offset) in self.inner.by_ref() {
            if ts <= &self.ts
                && matches!(
                    self.item_buf.as_ref().map(|(k, _)| *k != key.as_ref()),
                    Some(true) | None
                )
            {
                let g = IndexBatch::<K, T>::decode_value::<V>(self.batch, *offset)
                    .await?
                    .map(|v| (self.f)(&v));
                if let Some(value) = self.item_buf.replace((key, g)) {
                    return Ok(Some(value));
                }
            }
        }
        Ok(self.item_buf.take())
    }
}
