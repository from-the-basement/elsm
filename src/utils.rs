use std::cmp::Ordering;

pub(crate) struct CmpKeyItem<K: Ord, V> {
    pub(crate) key: K,
    pub(crate) _value: V,
}

impl<K, V> Eq for CmpKeyItem<K, V> where K: Ord {}

impl<K, V> PartialEq<Self> for CmpKeyItem<K, V>
where
    K: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> PartialOrd<Self> for CmpKeyItem<K, V>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V> Ord for CmpKeyItem<K, V>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
