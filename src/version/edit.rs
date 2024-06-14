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
    Add { level: u8, scope: Scope<K> },
    Remove { level: u8, gen: ProcessUniqueId },
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
            VersionEdit::Add { scope, level } => {
                writer.write_all(&0u8.to_le_bytes()).await?;
                writer.write_all(&level.to_le_bytes()).await?;
                scope.encode(writer).await?;
            }
            VersionEdit::Remove { gen, level } => {
                writer.write_all(&1u8.to_le_bytes()).await?;
                writer.write_all(&level.to_le_bytes()).await?;
                writer.write_all(&bincode::serialize(gen).unwrap()).await?;
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<u8>()
            + size_of::<u8>()
            + match self {
                VersionEdit::Add { scope, .. } => scope.size(),
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
        let level = {
            let mut level = [0; size_of::<u8>()];
            reader.read_exact(&mut level).await?;
            u8::from_le_bytes(level)
        };

        Ok(match edit_type {
            0 => {
                let scope = Scope::<K>::decode(reader).await?;

                VersionEdit::Add { level, scope }
            }
            1 => {
                let gen = {
                    let mut slice = [0; 16];
                    reader.read_exact(&mut slice).await?;
                    bincode::deserialize(&slice).unwrap()
                };
                VersionEdit::Remove { level, gen }
            }
            _ => todo!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{executor::block_on, io::Cursor};

    use crate::{scope::Scope, serdes::Encode, version::edit::VersionEdit};

    #[test]
    fn encode_and_decode() {
        block_on(async {
            let edits = vec![
                VersionEdit::Add {
                    level: 0,
                    scope: Scope {
                        min: Arc::new("Min".to_string()),
                        max: Arc::new("Max".to_string()),
                        gen: Default::default(),
                    },
                },
                VersionEdit::Remove {
                    level: 1,
                    gen: Default::default(),
                },
            ];

            let bytes = {
                let mut cursor = Cursor::new(vec![]);

                for edit in edits.clone() {
                    edit.encode(&mut cursor).await.unwrap();
                }
                cursor.into_inner()
            };

            let decode_edits = {
                let mut cursor = Cursor::new(bytes);

                VersionEdit::<String>::recover(&mut cursor).await
            };

            assert_eq!(edits, decode_edits);
        })
    }
}
