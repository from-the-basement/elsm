const JUMP: u64 = 1 << 31;

// rust version of https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
pub(crate) fn jump_consistent_hash(key: u64, buckets: usize) -> u32 {
    let mut k = key;
    let (mut b, mut j) = (-1, 0);
    while j < buckets as i64 {
        b = j;
        k = k.wrapping_mul(2862933555777941757) + 1;
        j = ((b + 1) as f64 * (JUMP as f64 / ((k >> 33) + 1) as f64)) as i64;
    }
    b as u32
}

#[cfg(test)]
mod tests {
    #[test]
    fn jump_consistent_hash() {
        let x = "the answer of life, universe and everything";
        let buckets = 42;
        let hash = super::jump_consistent_hash(x.len() as u64, buckets);
        assert!(hash < buckets as u32);
    }
}
