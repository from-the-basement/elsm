use std::{
    collections::{BTreeMap, Bound},
    sync::Arc,
};

use arrow::array::{AsArray, RecordBatch};

use crate::{mem_table::InternalKey, Offset};

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
    T: Ord + Copy,
{
    pub(crate) fn find(&self, key: &Arc<K>, ts: &T) -> Option<&[u8]> {
        let internal_key = InternalKey {
            key: key.clone(),
            ts: *ts,
        };
        self.index
            .range((Bound::Included(&internal_key), Bound::Unbounded))
            .next()
            .and_then(|(InternalKey { key: item_key, .. }, offset)| {
                (item_key == key).then(|| {
                    self.batch
                        .column(1)
                        .as_binary::<Offset>()
                        .value(*offset as usize)
                })
            })
    }
}
