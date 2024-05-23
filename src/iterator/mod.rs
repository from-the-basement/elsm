use std::{pin::Pin, sync::Arc};

use executor::futures::Stream;

pub(crate) mod buf_iterator;
pub(crate) mod merge_iterator;

type PinStream<'stream, K, G, E> =
    Pin<Box<dyn Stream<Item = Result<(Arc<K>, Option<G>), E>> + Send + 'stream>>;
