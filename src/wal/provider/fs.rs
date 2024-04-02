use std::{
    io,
    path::{Path, PathBuf},
};

use super::WalProvider;

pub struct Fs {
    path: PathBuf,
}

impl Fs {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        std::fs::read_dir(path.as_ref())?;
        Ok(Self {
            path: path.as_ref().to_owned(),
        })
    }
}

impl WalProvider for Fs {
    type File = executor::fs::File;

    async fn open(&self, fid: u32) -> io::Result<Self::File> {
        Ok(std::fs::File::open(self.path.join(format!("{}.wal", fid)))?.into())
    }
}
