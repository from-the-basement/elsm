use std::{
    collections::VecDeque,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::Stream;
use futures::Future;
use pin_project::pin_project;
use snowflake::ProcessUniqueId;

use crate::{
    serdes::{Decode, Encode},
    stream::{table_stream::TableStream, StreamError},
    DbOption,
};

#[pin_project]
pub(crate) struct LevelStream<'stream, K, V>
where
    K: Encode + Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    lower: Option<Arc<K>>,
    upper: Option<Arc<K>>,
    option: &'stream DbOption,
    gens: VecDeque<ProcessUniqueId>,
    stream: Option<TableStream<'stream, K, V>>,
}

impl<'stream, K, V> LevelStream<'stream, K, V>
where
    K: Encode + Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    pub(crate) async fn new(
        option: &'stream DbOption,
        gens: Vec<ProcessUniqueId>,
        lower: Option<&Arc<K>>,
        upper: Option<&Arc<K>>,
    ) -> Result<Self, StreamError<K, V>> {
        let mut gens = VecDeque::from(gens);
        let mut stream = None;

        if let Some(gen) = gens.pop_front() {
            stream = Some(TableStream::<K, V>::new(option, &gen, lower, upper).await?);
        }

        Ok(Self {
            lower: lower.cloned(),
            upper: upper.cloned(),
            option,
            gens,
            stream,
        })
    }
}

impl<K, V> Stream for LevelStream<'_, K, V>
where
    K: Encode + Decode + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    type Item = Result<(Arc<K>, Option<V>), StreamError<K, V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(stream) = &mut self.stream {
            return match Pin::new(stream).poll_next(cx) {
                Poll::Ready(None) => match self.gens.pop_front() {
                    None => Poll::Ready(None),
                    Some(gen) => {
                        let min = self.lower.clone();
                        let max = self.upper.clone();
                        let mut future = pin!(TableStream::<K, V>::new(
                            self.option,
                            &gen,
                            min.as_ref(),
                            max.as_ref()
                        ));

                        match future.as_mut().poll(cx) {
                            Poll::Ready(Ok(stream)) => {
                                self.stream = Some(stream);
                                self.poll_next(cx)
                            }
                            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                            Poll::Pending => Poll::Pending,
                        }
                    }
                },
                poll => poll,
            };
        }
        Poll::Ready(None)
    }
}
