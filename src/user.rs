use std::{io, sync::Arc};

use arrow::{
    array::{
        Array, RecordBatch, StringArray, StringBuilder, StructArray, StructBuilder, UInt64Array,
        UInt64Builder,
    },
    datatypes::{DataType, Field, Fields, SchemaRef},
};
use elsm_marco::elsm_schema;
use executor::futures::{AsyncRead, AsyncWrite};
use lazy_static::lazy_static;

use crate::{
    schema::{Builder, Schema},
    serdes::{Decode, Encode},
};

#[derive(Debug, Eq, PartialEq)]
#[elsm_schema]
pub(crate) struct User {
    #[primary_key]
    pub(crate) id: u64,
    pub(crate) name: String,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::ExecutorBuilder;
    use tempfile::TempDir;

    use crate::{
        oracle::LocalOracle, schema::Schema, user::UserInner, wal::provider::in_mem::InMemProvider,
        Db, DbOption,
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
            let user_0 = UserInner::new(0, "lizeren".to_string());
            let user_1 = UserInner::new(1, "2333".to_string());
            let user_2 = UserInner::new(2, "ghost".to_string());

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
