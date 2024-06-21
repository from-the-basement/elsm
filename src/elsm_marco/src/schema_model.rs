use proc_macro2::Ident;
use syn::{parse::Result, Field};

use crate::keys::KeyDefinition;

pub(crate) struct ModelAttributes {
    pub(crate) struct_name: Ident,
    pub(crate) primary_key: Option<KeyDefinition>,
}

impl ModelAttributes {
    pub(crate) fn parse_field(&mut self, field: &Field) -> Result<bool> {
        for attr in &field.attrs {
            if attr.path.is_ident("primary_key") {
                self.primary_key = Some(KeyDefinition {
                    struct_name: self.struct_name.clone(),
                    field_name: field.ident.clone().unwrap(),
                });
                return Ok(true);
            }
        }
        Ok(false)
    }
}
