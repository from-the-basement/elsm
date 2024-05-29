use std::sync::Arc;

use snowflake::ProcessUniqueId;

use crate::serdes::Encode;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Scope<K>
where
    K: Encode + Ord,
{
    pub(crate) min: Arc<K>,
    pub(crate) max: Arc<K>,
    pub(crate) gen: ProcessUniqueId,
}

impl<K> Scope<K>
where
    K: Encode + Ord,
{
    pub(crate) fn is_between(&self, key: &K) -> bool {
        self.min.as_ref().le(key) && self.max.as_ref().ge(key)
    }

    pub(crate) fn is_meet(&self, target: &Scope<K>) -> bool {
        (self.min.le(&target.min) && self.max.ge(&target.min))
            || (self.min.le(&target.max) && self.max.ge(&target.max))
            || (self.min.le(&target.min)) && self.max.ge(&target.max)
            || (self.min.ge(&target.min)) && self.max.le(&target.max)
    }
}
