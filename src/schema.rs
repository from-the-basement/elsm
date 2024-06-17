use crate::serdes::{Decode, Encode};

pub trait Schema {
    type PrimaryKey: Ord + Encode + Decode;

    fn arrow_schema() -> arrow::datatypes::Schema;

    fn primary_key(&self) -> Self::PrimaryKey;
}
