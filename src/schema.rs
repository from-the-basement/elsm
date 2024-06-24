use std::{fmt::Debug, hash::Hash};

use arrow::{
    array::{Array, RecordBatch},
    datatypes::SchemaRef,
};

use crate::serdes::{Decode, Encode};

pub trait Schema: Debug + Clone + Encode + Decode + 'static {
    type PrimaryKey: Debug + Clone + Ord + Hash + Encode + Decode + 'static;
    type Builder: Builder<Self> + Send;
    type PrimaryKeyArray: Array;

    fn arrow_schema() -> SchemaRef;

    fn inner_schema() -> SchemaRef;

    fn primary_key(&self) -> Self::PrimaryKey;

    fn builder() -> Self::Builder;

    fn from_batch(batch: &RecordBatch, offset: usize) -> (Self::PrimaryKey, Option<Self>);

    fn to_primary_key_array(keys: Vec<Self::PrimaryKey>) -> Self::PrimaryKeyArray;
}

pub trait Builder<S: Schema> {
    fn add(&mut self, primary_key: &S::PrimaryKey, schema: Option<S>);

    fn finish(&mut self) -> RecordBatch;
}
