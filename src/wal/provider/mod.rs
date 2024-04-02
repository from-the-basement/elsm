pub mod fs;
pub mod in_mem;

use std::{future::Future, io};

pub trait WalProvider: Send + Sync + 'static {
    type File: Unpin + Send + 'static;

    fn open(&self, fid: u32) -> impl Future<Output = io::Result<Self::File>>;
}
