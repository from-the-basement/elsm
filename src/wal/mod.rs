mod checksum;
pub mod provider;

use std::{
    error::Error,
    future::Future,
    io,
    marker::PhantomData,
    sync::atomic::{AtomicU32, Ordering},
};

use async_stream::stream;
use checksum::{HashReader, HashWriter};
use futures::{
    io::{BufReader, BufWriter},
    AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, Stream,
};
use thiserror::Error;

use self::provider::WalProvider;
use crate::{
    record::Record,
    serdes::{Decode, Encode},
};

#[derive(Debug)]
pub(crate) struct WalManager<WP> {
    pub(crate) wal_provider: WP,
    file_id: AtomicU32,
}

impl<WP> WalManager<WP>
where
    WP: WalProvider,
{
    pub(crate) fn new(wal_provider: WP) -> Self {
        Self {
            wal_provider,
            file_id: AtomicU32::new(0),
        }
    }

    pub(crate) async fn create_wal_file<K, V, T>(&self) -> io::Result<WalFile<WP::File, K, V, T>> {
        let file_id = self.file_id.fetch_add(1, Ordering::Relaxed);
        let file = self.wal_provider.open(file_id).await?;

        self.pack_wal_file(file).await
    }

    pub(crate) async fn pack_wal_file<K, V, T>(
        &self,
        file: WP::File,
    ) -> io::Result<WalFile<WP::File, K, V, T>> {
        Ok(WalFile::new(file))
    }
}

pub trait WalWrite<K, V, T>
where
    K: Encode,
    V: Encode,
    T: Encode,
{
    fn write(
        &mut self,
        record: Record<&K, &V, &T>,
    ) -> impl Future<Output = Result<(), WriteError<<Record<&K, &V, &T> as Encode>::Error>>>;

    fn flush(&mut self) -> impl Future<Output = io::Result<()>>;

    fn close(self) -> impl Future<Output = io::Result<()>>;
}

pub trait WalRecover<K, V, T> {
    type Error: std::error::Error + Send + Sync + 'static;

    fn recover(&mut self) -> impl Stream<Item = Result<Record<K, V, T>, Self::Error>>;
}

#[derive(Debug)]
pub(crate) struct WalFile<F, K, V, T> {
    file: F,
    _marker: PhantomData<(K, V, T)>,
}

impl<F, K, V, T> WalFile<F, K, V, T> {
    pub(crate) fn new(file: F) -> Self {
        Self {
            file,
            _marker: PhantomData,
        }
    }
}

impl<F, K, V, T> WalWrite<K, V, T> for WalFile<F, K, V, T>
where
    F: AsyncWrite + Unpin + Send + Sync,
    K: Encode,
    V: Encode,
    T: Encode,
{
    async fn write(
        &mut self,
        record: Record<&K, &V, &T>,
    ) -> Result<(), WriteError<<Record<&K, &V, &T> as Encode>::Error>> {
        let mut writer = HashWriter::new(&mut self.file);
        record.encode(&mut writer).await?;
        writer.eol().await.map_err(WriteError::Io)?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.file.flush().await
    }

    async fn close(mut self) -> io::Result<()> {
        self.file.flush().await?;
        self.file.close().await
    }
}

impl<F, K, V, T> WalRecover<K, V, T> for WalFile<F, K, V, T>
where
    F: AsyncRead + Unpin,
    K: Decode,
    V: Decode,
    T: Decode,
{
    type Error = RecoverError<<Record<K, V, T> as Decode>::Error>;

    fn recover(&mut self) -> impl Stream<Item = Result<Record<K, V, T>, Self::Error>> {
        stream! {
            // Safety: https://github.com/rust-lang/futures-rs/pull/2848 fix this, waiting for release
            let mut file = BufReader::new(unsafe { std::mem::transmute::<_, &mut F>(std::mem::transmute::<_, &mut BufWriter<Vec<_>>>(&mut self.file).get_mut()) });

            loop {
                if file.buffer().is_empty() && file.fill_buf().await.map_err(RecoverError::Io)?.is_empty() {
                    return;
                }

                let mut reader = HashReader::new(&mut file);

                let record = Record::decode(&mut reader).await?;

                if !reader.checksum().await.map_err(RecoverError::Io)? {
                    yield Err(RecoverError::Checksum);
                    return;
                }

                yield Ok(record);
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum WriteError<E: std::error::Error> {
    #[error("wal write encode error: {0}")]
    Encode(#[from] E),
    #[error("wal write io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("wal write max size exceeded")]
    MaxSizeExceeded,
    #[error("wal write arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("wal write internal error: {0}")]
    Internal(#[source] Box<dyn Error + Send + Sync + 'static>),
}

#[derive(Debug, Error)]
pub(crate) enum RecoverError<E: std::error::Error> {
    #[error("wal recover decode error: {0}")]
    Decode(#[from] E),
    #[error("wal recover checksum error")]
    Checksum,
    #[error("wal recover io error")]
    Io(#[source] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::{executor::block_on, io::Cursor, StreamExt};

    use super::{Record, WalFile, WalRecover, WalWrite};
    use crate::record::RecordType;

    #[test]
    fn write_and_recover() {
        let mut file = Vec::new();
        block_on(async {
            {
                let mut wal = WalFile::new(Cursor::new(&mut file));
                wal.write(Record::new(
                    RecordType::Full,
                    &"key".to_string(),
                    &0_u64,
                    Some(&"value".to_string()),
                ))
                .await
                .unwrap();
                wal.flush().await.unwrap();
            }
            {
                let mut wal = WalFile::new(Cursor::new(&mut file));

                {
                    let mut stream = pin!(wal.recover());
                    assert_eq!(
                        stream.next().await.unwrap().unwrap().value,
                        Some("value".to_string())
                    );
                }

                wal.write(Record::new(
                    RecordType::Full,
                    &"key".to_string(),
                    &0_u64,
                    Some(&"value".to_string()),
                ))
                .await
                .unwrap();
                wal.flush().await.unwrap();
            }

            {
                let mut wal = WalFile::new(Cursor::new(&mut file));

                {
                    let mut stream = pin!(wal.recover());
                    let record: Record<String, _> = stream.next().await.unwrap().unwrap();
                    assert_eq!(record.key, "key".to_string());
                    assert_eq!(record.value, Some("value".to_string()));
                    let record: Record<String, _> = stream.next().await.unwrap().unwrap();
                    assert_eq!(record.key, "key".to_string());
                    assert_eq!(record.value, Some("value".to_string()));
                }
            }
        });
    }
}
