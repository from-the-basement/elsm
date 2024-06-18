use std::{
    collections::{btree_map::Range, Bound},
    fmt::Debug,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use arrow::array::RecordBatch;
use executor::futures::{Stream, StreamExt};
use pin_project::pin_project;

use crate::{
    index_batch::IndexBatch, mem_table::InternalKey, oracle::TimeStamp, schema::Schema,
    stream::StreamError,
};

#[pin_project]
#[derive(Debug)]
pub(crate) struct IndexBatchStream<'a, S, G, F>
where
    S: Schema,
    G: Send + 'static,
    F: Fn(&S) -> G + Sync + 'static,
{
    batch: &'a RecordBatch,
    item_buf: Option<(Arc<S::PrimaryKey>, Option<G>)>,
    inner: Range<'a, InternalKey<S::PrimaryKey>, u32>,
    ts: TimeStamp,
    f: F,
}

impl<'a, S, G, F> Stream for IndexBatchStream<'a, S, G, F>
where
    S: Schema,
    G: Send + 'static,
    F: Fn(&S) -> G + Sync + 'static,
{
    type Item = Result<(Arc<S::PrimaryKey>, Option<G>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        for (InternalKey { key, ts }, offset) in this.inner.by_ref() {
            if ts <= this.ts
                && matches!(
                    this.item_buf.as_ref().map(|(k, _)| k != key),
                    Some(true) | None
                )
            {
                return Poll::Ready(
                    this.item_buf
                        .replace((
                            key.clone(),
                            S::from_batch(this.batch, *offset as usize)
                                .1
                                .map(|v| (this.f)(&v)),
                        ))
                        .map(Ok),
                );
            }
        }
        Poll::Ready(this.item_buf.take().map(Ok))
    }
}

impl<S> IndexBatch<S>
where
    S: Schema,
{
    pub(crate) async fn range<G, F>(
        &self,
        lower: Option<&Arc<S::PrimaryKey>>,
        upper: Option<&Arc<S::PrimaryKey>>,
        ts: &TimeStamp,
        f: F,
    ) -> Result<IndexBatchStream<S, G, F>, StreamError<S::PrimaryKey, S>>
    where
        G: Send + 'static,
        F: Fn(&S) -> G + Sync + 'static,
    {
        let mut iterator = IndexBatchStream {
            batch: &self.batch,
            inner: self.index.range((
                lower
                    .map(|k| {
                        Bound::Included(InternalKey {
                            key: k.clone(),
                            ts: *ts,
                        })
                    })
                    .unwrap_or(Bound::Unbounded),
                upper
                    .map(|k| {
                        Bound::Included(InternalKey {
                            key: k.clone(),
                            ts: TimeStamp::default(),
                        })
                    })
                    .unwrap_or(Bound::Unbounded),
            )),
            item_buf: None,
            ts: *ts,
            f,
        };

        {
            let mut iterator = pin!(&mut iterator);
            // filling first item
            let _ = iterator.next().await;
        }

        Ok(iterator)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::futures::StreamExt;
    use futures::executor::block_on;

    use crate::{
        mem_table::MemTable, oracle::LocalOracle, user::User, wal::provider::in_mem::InMemProvider,
        Db,
    };

    #[test]
    fn range() {
        block_on(async {
            let mut mem_table = MemTable::<User>::default();

            mem_table.insert(Arc::new(0), 0, None);
            mem_table.insert(
                Arc::new(1),
                0,
                Some(User {
                    id: 1,
                    name: "1".to_string(),
                }),
            );
            mem_table.insert(Arc::new(1), 1, None);
            mem_table.insert(
                Arc::new(2),
                0,
                Some(User {
                    id: 2,
                    name: "2".to_string(),
                }),
            );
            mem_table.insert(Arc::new(3), 0, None);

            let batch = Db::<User, LocalOracle<u64>, InMemProvider>::freeze(mem_table)
                .await
                .unwrap();

            let mut iterator = batch
                .range(Some(&Arc::new(1)), Some(&Arc::new(2)), &1, |v: &User| {
                    v.clone()
                })
                .await
                .unwrap();

            assert_eq!(iterator.next().await.unwrap().unwrap(), (Arc::new(1), None));
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(2),
                    Some(User {
                        id: 2,
                        name: "2".to_string()
                    })
                )
            );
            assert!(iterator.next().await.is_none())
        })
    }
}
