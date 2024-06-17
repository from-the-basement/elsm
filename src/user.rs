use std::io;

use arrow::datatypes::{DataType, Field};
use executor::futures::{AsyncRead, AsyncWrite};

use crate::{
    schema::Schema,
    serdes::{Decode, Encode},
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct User {
    pub(crate) id: u64,
    pub(crate) name: String,
}

impl Schema for User {
    type PrimaryKey = u64;

    fn arrow_schema() -> arrow::datatypes::Schema {
        arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }
}

impl Encode for User {
    type Error = io::Error;

    async fn encode<W: AsyncWrite + Unpin + Send + Sync>(
        &self,
        writer: &mut W,
    ) -> Result<(), Self::Error> {
        self.id.encode(writer).await?;
        self.name.encode(writer).await?;

        Ok(())
    }

    fn size(&self) -> usize {
        self.id.size() + self.name.size()
    }
}

impl Decode for User {
    type Error = io::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let id = u64::decode(reader).await?;
        let name = String::decode(reader).await?;

        Ok(User { id, name })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::ExecutorBuilder;
    use tempfile::TempDir;

    use crate::{
        oracle::LocalOracle, schema::Schema, user::User, wal::provider::in_mem::InMemProvider, Db,
        DbOption,
    };

    #[test]
    fn test_user() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption::new(temp_dir.path().to_path_buf()),
                )
                .await
                .unwrap(),
            );
            let user_0 = User {
                id: 0,
                name: "lizeren".to_string(),
            };
            let user_1 = User {
                id: 1,
                name: "2333".to_string(),
            };
            let user_2 = User {
                id: 1,
                name: "ghost".to_string(),
            };

            let mut t0 = db.new_txn();

            t0.set(user_0.primary_key(), user_0.clone());
            t0.set(user_1.primary_key(), user_1.clone());
            t0.set(user_2.primary_key(), user_2.clone());

            t0.commit().await.unwrap();

            let txn = db.new_txn();

            assert_eq!(
                txn.get(&Arc::new(user_0.primary_key()), |v| v.clone())
                    .await,
                Some(user_0)
            );
            assert_eq!(
                txn.get(&Arc::new(user_1.primary_key()), |v| v.clone())
                    .await,
                Some(user_1)
            );
            assert_eq!(
                txn.get(&Arc::new(user_2.primary_key()), |v| v.clone())
                    .await,
                Some(user_2)
            );
        });
    }
}
