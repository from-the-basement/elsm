use std::mem::size_of;

use executor::futures::{
    util::{AsyncReadExt, AsyncWriteExt},
    AsyncRead, AsyncWrite,
};
use snowflake::ProcessUniqueId;

use crate::{
    scope::Scope,
    serdes::{Decode, Encode},
};

#[derive(Debug, Clone, Eq, PartialEq)]
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
    pub(crate) async fn recover<R: AsyncRead + Unpin>(reader: &mut R) -> Vec<VersionEdit<K>> {
        let mut edits = Vec::new();

        while let Ok(edit) = VersionEdit::decode(reader).await {
            edits.push(edit)
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
                scope: Scope::<K>::decode(reader).await?,
            },
            1 => {
                let gen = {
                    let mut slice = [0; 16];
                    reader.read_exact(&mut slice).await?;
                    bincode::deserialize(&slice).unwrap()
                };
                VersionEdit::Remove { gen }
            }
            _ => todo!(),
        })
    }
}
