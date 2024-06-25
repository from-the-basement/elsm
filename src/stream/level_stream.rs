use std::{
    collections::VecDeque,
    pin::{pin, Pin},
    task::{Context, Poll},
};

use executor::futures::Stream;
use futures::Future;
use pin_project::pin_project;
use snowflake::ProcessUniqueId;

use crate::{
    schema::Schema,
    stream::{table_stream::TableStream, StreamError},
    DbOption,
};

#[pin_project]
pub(crate) struct LevelStream<'stream, S>
where
    S: Schema,
{
    lower: Option<S::PrimaryKey>,
    upper: Option<S::PrimaryKey>,
    option: &'stream DbOption,
    gens: VecDeque<ProcessUniqueId>,
    stream: Option<TableStream<'stream, S>>,
}

impl<'stream, S> LevelStream<'stream, S>
where
    S: Schema,
{
    pub(crate) async fn new(
        option: &'stream DbOption,
        gens: Vec<ProcessUniqueId>,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
    ) -> Result<Self, StreamError<S::PrimaryKey, S>> {
        let mut gens = VecDeque::from(gens);
        let mut stream = None;

        if let Some(gen) = gens.pop_front() {
            stream = Some(TableStream::<S>::new(option, &gen, lower, upper).await?);
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

impl<S> Stream for LevelStream<'_, S>
where
    S: Schema,
{
    type Item = Result<(S::PrimaryKey, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(stream) = &mut self.stream {
            return match Pin::new(stream).poll_next(cx) {
                Poll::Ready(None) => match self.gens.pop_front() {
                    None => Poll::Ready(None),
                    Some(gen) => {
                        let min = self.lower.clone();
                        let max = self.upper.clone();
                        let mut future = pin!(TableStream::<S>::new(
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
