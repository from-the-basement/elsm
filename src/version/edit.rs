use crate::scope::Scope;
use crate::serdes::{Decode, Encode};
use executor::fs::File;
use executor::futures::util::{AsyncReadExt, AsyncWriteExt};
use executor::futures::{AsyncRead, AsyncWrite};
use parquet::data_type::AsBytes;
use snowflake::ProcessUniqueId;
use std::io;
use std::mem::size_of;

pub(crate) enum VersionEdit<K>
where
    K: Encode + Decode + Ord,
{
    Add { scope: Scope<K> },
    Remove { gen: ProcessUniqueId },
}

impl<K> VersionEdit<K>
where
    K: Encode + Decode + Ord,
{
    pub(crate) async fn recover(log: &mut File) -> Vec<VersionEdit<K>> {
        let mut edits = Vec::new();

        loop {
            match VersionEdit::decode(log).await {
                Ok(edit) => edits.push(edit),
                Err(_) => break,
            }
        }
        edits
    }
}

impl<K> Encode for VersionEdit<K>
where
    K: Encode + Decode + Ord,
{
    type Error = <K as Encode>::Error;

    async fn encode<W: AsyncWrite + Unpin + Send + Sync>(
        &self,
        writer: &mut W,
    ) -> Result<(), Self::Error> {
        writer
            .write_all(
                match self {
                    VersionEdit::Add { .. } => 0u8,
                    VersionEdit::Remove { .. } => 1u8,
                }
                .as_bytes(),
            )
            .await?;

        match self {
            VersionEdit::Add { scope } => {
                writer.write_all(&0u8.to_le_bytes()).await?;
                scope.encode(writer).await?;
            }
            VersionEdit::Remove { gen } => {
                writer.write_all(&1u8.to_le_bytes()).await?;
                writer.write_all(&bincode::serialize(gen).unwrap()).await?;
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<u8>()
            + match self {
                VersionEdit::Add { scope } => scope.size(),
                VersionEdit::Remove { .. } => 16,
            }
    }
}

impl<K> Decode for VersionEdit<K>
where
    K: Encode + Decode + Ord,
{
    type Error = <K as Decode>::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let edit_type = {
            let mut len = [0; size_of::<u8>()];
            reader.read_exact(&mut len).await?;
            u8::from_le_bytes(len) as usize
        };

        Ok(match edit_type {
            0 => VersionEdit::Add {
                // FIXME: unwrap
                scope: Scope::decode(reader).await.unwrap(),
            },
            1 => {
                let gen = {
                    let mut slice = [0; 12];
                    reader.read_exact(&mut slice).await?;
                    bincode::deserialize(&slice).unwrap()
                };
                VersionEdit::Remove { gen }
            }
            _ => todo!(),
        })
    }
}
