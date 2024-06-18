use std::{
    collections::{btree_map, Bound},
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::{util::StreamExt, Stream};
use pin_project::pin_project;

use crate::{
    mem_table::{InternalKey, MemTable},
    oracle::TimeStamp,
    schema::Schema,
    stream::StreamError,
};

#[pin_project]
pub(crate) struct MemTableStream<'a, S, G, F>
where
    S: Schema,
    G: Send + Sync + 'static,
    F: Fn(&S) -> G + Sync + 'static,
{
    inner: btree_map::Range<'a, InternalKey<S::PrimaryKey>, Option<S>>,
    item_buf: Option<(Arc<S::PrimaryKey>, Option<G>)>,
    ts: TimeStamp,
    f: F,
}

impl<'a, S, G, F> Stream for MemTableStream<'a, S, G, F>
where
    S: Schema,
    G: Send + Sync + 'static,
    F: Fn(&S) -> G + Sync + 'static,
{
    type Item = Result<(Arc<S::PrimaryKey>, Option<G>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        for (InternalKey { key, ts }, value) in this.inner.by_ref() {
            if ts <= this.ts
                && matches!(
                    this.item_buf.as_ref().map(|(k, _)| k != key),
                    Some(true) | None
                )
            {
                return Poll::Ready(
                    this.item_buf
                        .replace((key.clone(), value.as_ref().map(|v| (this.f)(v))))
                        .map(Ok),
                );
            }
        }
        Poll::Ready(this.item_buf.take().map(Ok))
    }
}

impl<S> MemTable<S>
where
    S: Schema,
{
    pub(crate) async fn iter<G, F>(
        &self,
        f: F,
    ) -> Result<MemTableStream<S, G, F>, StreamError<S::PrimaryKey, S>>
    where
        G: Send + Sync + 'static,
        F: Fn(&S) -> G + Sync + 'static,
    {
        let mut iterator = MemTableStream {
            inner: self.data.range::<InternalKey<S::PrimaryKey>, (
                Bound<InternalKey<S::PrimaryKey>>,
                Bound<InternalKey<S::PrimaryKey>>,
            )>((Bound::Unbounded, Bound::Unbounded)),
            item_buf: None,
            ts: self.max_ts,
            f,
        };
        {
            let mut iterator = pin!(&mut iterator);
            // filling first item
            let _ = iterator.next().await;
        }
        Ok(iterator)
    }

    pub(crate) async fn range<G, F>(
        &self,
        lower: Option<&Arc<S::PrimaryKey>>,
        upper: Option<&Arc<S::PrimaryKey>>,
        ts: &TimeStamp,
        f: F,
    ) -> Result<MemTableStream<S, G, F>, StreamError<S::PrimaryKey, S>>
    where
        G: Send + Sync + 'static,
        F: Fn(&S) -> G + Sync + 'static,
    {
        let mut iterator = MemTableStream {
            inner: self.data.range((
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

    use executor::futures::{future::block_on, StreamExt};

    use crate::{mem_table::MemTable, user::User};

    #[test]
    fn iterator() {
        block_on(async {
            let mut mem_table = MemTable::default();

            mem_table.insert(
                Arc::new(1),
                0,
                Some(User {
                    id: 1,
                    name: "1".to_string(),
                }),
            );
            mem_table.insert(
                Arc::new(1),
                1,
                Some(User {
                    id: 2,
                    name: "2".to_string(),
                }),
            );

            mem_table.insert(
                Arc::new(2),
                0,
                Some(User {
                    id: 1,
                    name: "1".to_string(),
                }),
            );

            let mut iterator = mem_table.iter(|v| v.clone()).await.unwrap();

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(1),
                    Some(User {
                        id: 2,
                        name: "2".to_string()
                    })
                )
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(2),
                    Some(User {
                        id: 1,
                        name: "1".to_string()
                    })
                )
            );

            drop(iterator);
            mem_table.insert(Arc::new(1), 3, None);

            let mut iterator = mem_table.iter(|v| v.clone()).await.unwrap();

            assert_eq!(iterator.next().await.unwrap().unwrap(), (Arc::new(1), None));
        });
    }

    #[test]
    fn range() {
        futures::executor::block_on(async {
            let mut mem_table = MemTable::default();

            mem_table.insert(
                Arc::new(1),
                0,
                Some(User {
                    id: 1,
                    name: "1".to_string(),
                }),
            );
            mem_table.insert(
                Arc::new(2),
                0,
                Some(User {
                    id: 2,
                    name: "2".to_string(),
                }),
            );
            mem_table.insert(
                Arc::new(2),
                1,
                Some(User {
                    id: 3,
                    name: "3".to_string(),
                }),
            );
            mem_table.insert(
                Arc::new(3),
                0,
                Some(User {
                    id: 3,
                    name: "3".to_string(),
                }),
            );
            mem_table.insert(
                Arc::new(4),
                0,
                Some(User {
                    id: 4,
                    name: "4".to_string(),
                }),
            );

            let mut iterator = mem_table.iter(|v| v.clone()).await.unwrap();

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(1),
                    Some(User {
                        id: 1,
                        name: "1".to_string()
                    })
                )
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(2),
                    Some(User {
                        id: 3,
                        name: "3".to_string()
                    })
                )
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(3),
                    Some(User {
                        id: 3,
                        name: "3".to_string()
                    })
                )
            );
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(4),
                    Some(User {
                        id: 4,
                        name: "4".to_string()
                    })
                )
            );
            assert!(iterator.next().await.is_none());

            let mut iterator = mem_table
                .range(Some(&Arc::new(2)), Some(&Arc::new(3)), &0, |v| v.clone())
                .await
                .unwrap();

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
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    Arc::new(3),
                    Some(User {
                        id: 3,
                        name: "3".to_string()
                    })
                )
            );
            assert!(iterator.next().await.is_none())
        });
    }
}
