pub mod fs;
pub mod in_mem;

use std::{future::Future, io};

use executor::futures::Stream;

pub trait WalProvider: Send + Sync + 'static {
    type File: Unpin + Send + Sync + 'static;

    fn open(&self, fid: u32) -> impl Future<Output = io::Result<Self::File>>;

    fn list(&self) -> impl Stream<Item = io::Result<Self::File>>;
}
