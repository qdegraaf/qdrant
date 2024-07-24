use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use super::binary_index::BinaryIndex;
use super::map_index::MapIndex;
use super::FieldIndexBuilder;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::numeric_index::NumericIndex;
use crate::index::field_index::FieldIndex;
use crate::json_path::JsonPath;
use crate::types::{PayloadFieldSchema, PayloadSchemaParams};

/// Selects index types based on field type
pub fn index_selector(
    field: &JsonPath,
    payload_schema: &PayloadFieldSchema,
    db: &Arc<RwLock<DB>>,
    mmap_index_dir: Option<PathBuf>,
    is_appendable: bool,
) -> OperationResult<Vec<FieldIndex>> {
    let column: String = field.to_string();

    Ok(match payload_schema.expand().as_ref() {
        PayloadSchemaParams::Keyword(_) => vec![FieldIndex::KeywordIndex(MapIndex::new(
            db.clone(),
            &column,
            is_appendable,
        ))],
        PayloadSchemaParams::Integer(integer_params) => itertools::chain(
            integer_params.lookup.then(|| {
                FieldIndex::IntMapIndex(MapIndex::new(db.clone(), &column, is_appendable))
            }),
            integer_params
                .range
                .then(|| {
                    OperationResult::Ok(FieldIndex::IntIndex(match &mmap_index_dir {
                        Some(dir) => NumericIndex::new_mmap(dir)?,
                        None => NumericIndex::new(db.clone(), &column, is_appendable),
                    }))
                })
                .transpose()?,
        )
        .collect(),
        PayloadSchemaParams::Float(_) => {
            vec![FieldIndex::FloatIndex(match &mmap_index_dir {
                Some(dir) => NumericIndex::new_mmap(dir)?,
                None => NumericIndex::new(db.clone(), &column, is_appendable),
            })]
        }
        PayloadSchemaParams::Geo(_) => vec![FieldIndex::GeoIndex(GeoMapIndex::new(
            db.clone(),
            &column,
            is_appendable,
        ))],
        PayloadSchemaParams::Text(text_index_params) => {
            vec![FieldIndex::FullTextIndex(FullTextIndex::new(
                db.clone(),
                text_index_params.clone(),
                &column,
                is_appendable,
            ))]
        }
        PayloadSchemaParams::Bool(_) => {
            vec![FieldIndex::BinaryIndex(BinaryIndex::new(
                db.clone(),
                &column,
            ))]
        }
        PayloadSchemaParams::Datetime(_) => {
            vec![FieldIndex::DatetimeIndex(match &mmap_index_dir {
                Some(dir) => NumericIndex::new_mmap(dir)?,
                None => NumericIndex::new(db.clone(), &column, is_appendable),
            })]
        }
        PayloadSchemaParams::Uuid(_) => {
            vec![FieldIndex::UuidIndex(match &mmap_index_dir {
                Some(dir) => NumericIndex::new_mmap(dir)?,
                None => NumericIndex::new(db.clone(), &column, is_appendable),
            })]
        }
    })
}

/// Selects index builder based on field type
pub fn index_builder_selector(
    field: &JsonPath,
    payload_schema: &PayloadFieldSchema,
    db: Arc<RwLock<DB>>,
    mmap_index_dir: Option<PathBuf>,
) -> Vec<FieldIndexBuilder> {
    let column = field.to_string();

    if let Some(dir) = &mmap_index_dir {
        create_dir_all(dir).unwrap(); // TODO
    }

    match payload_schema.expand().as_ref() {
        PayloadSchemaParams::Keyword(_) => vec![FieldIndexBuilder::KeywordIndex(
            MapIndex::builder(db, &column),
        )],
        PayloadSchemaParams::Integer(integer_params) => itertools::chain(
            integer_params
                .lookup
                .then(|| FieldIndexBuilder::IntMapIndex(MapIndex::builder(db.clone(), &column))),
            integer_params.range.then(|| match &mmap_index_dir {
                Some(dir) => FieldIndexBuilder::IntMmapIndex(NumericIndex::builder_mmap(dir)),
                None => FieldIndexBuilder::IntIndex(NumericIndex::builder(db, &column)),
            }),
        )
        .collect(),
        PayloadSchemaParams::Float(_) => {
            vec![match &mmap_index_dir {
                Some(dir) => FieldIndexBuilder::FloatMmapIndex(NumericIndex::builder_mmap(dir)),
                None => FieldIndexBuilder::FloatIndex(NumericIndex::builder(db, &column)),
            }]
        }
        PayloadSchemaParams::Geo(_) => {
            vec![FieldIndexBuilder::GeoIndex(GeoMapIndex::builder(
                db, &column,
            ))]
        }
        PayloadSchemaParams::Text(text_index_params) => vec![FieldIndexBuilder::FullTextIndex(
            FullTextIndex::builder(db, text_index_params.clone(), &column),
        )],
        PayloadSchemaParams::Bool(_) => {
            vec![FieldIndexBuilder::BinaryIndex(BinaryIndex::builder(
                db, &column,
            ))]
        }
        PayloadSchemaParams::Datetime(_) => {
            vec![match &mmap_index_dir {
                Some(dir) => FieldIndexBuilder::FloatMmapIndex(NumericIndex::builder_mmap(dir)),
                None => FieldIndexBuilder::FloatIndex(NumericIndex::builder(db, &column)),
            }]
        }
        PayloadSchemaParams::Uuid(_) => {
            vec![match &mmap_index_dir {
                Some(dir) => FieldIndexBuilder::DatetimeMmapIndex(NumericIndex::builder_mmap(dir)),
                None => FieldIndexBuilder::DatetimeIndex(NumericIndex::builder(db, &column)),
            }]
        }
    }
}
