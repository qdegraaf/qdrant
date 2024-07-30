use std::iter;
use std::path::{Path, PathBuf};

use ahash::HashMap;
use common::mmap_hashmap::{Key, MmapHashMap};
use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use memmap2::MmapMut;
use memory::mmap_ops::{self, create_and_ensure_length};
use serde::{Deserialize, Serialize};

use super::{IdRefIter, MapIndexKey};
use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::mmap_type::MmapBitSlice;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::index::field_index::mmap_point_to_values::MmapPointToValues;

const DELETED_PATH: &str = "deleted.bin";
const HASHMAP_PATH: &str = "values_to_points.bin";
const CONFIG_PATH: &str = "mmap_field_index_config.json";

pub struct MmapMapIndex<N: MapIndexKey + Key + ?Sized> {
    path: PathBuf,
    value_to_points: MmapHashMap<N>,
    point_to_values: MmapPointToValues<N>,
    deleted: MmapBitSliceBufferedUpdateWrapper,
    deleted_count: usize,
    total_key_value_pairs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MmapMapIndexConfig {
    total_key_value_pairs: usize,
}

impl<N: MapIndexKey + Key + ?Sized> MmapMapIndex<N> {
    pub fn load(path: &Path) -> OperationResult<Self> {
        let hashmap_path = path.join(HASHMAP_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        let config: MmapMapIndexConfig = read_json(&config_path)?;

        let hashmap = MmapHashMap::open(&hashmap_path)?;
        let point_to_values = MmapPointToValues::open(path)?;

        let deleted = mmap_ops::open_write_mmap(&deleted_path)?;
        let deleted = MmapBitSlice::from(deleted, 0);
        let deleted_count = deleted.count_ones();

        Ok(Self {
            path: path.to_path_buf(),
            value_to_points: hashmap,
            point_to_values,
            deleted: MmapBitSliceBufferedUpdateWrapper::new(deleted),
            deleted_count,
            total_key_value_pairs: config.total_key_value_pairs,
        })
    }

    pub fn build(
        path: &Path,
        point_to_values: Vec<Vec<N::Owned>>,
        values_to_points: HashMap<N::Owned, Vec<PointOffsetType>>,
    ) -> OperationResult<Self> {
        let hashmap_path = path.join(HASHMAP_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        atomic_save_json(
            &config_path,
            &MmapMapIndexConfig {
                total_key_value_pairs: point_to_values.iter().map(|v| v.len()).sum(),
            },
        )?;

        MmapHashMap::create(
            &hashmap_path,
            values_to_points
                .iter()
                .map(|(value, ids)| (<N as MapIndexKey>::from_owned(value), ids.iter().copied())),
        )?;

        MmapPointToValues::<N>::from_iter(
            path,
            point_to_values.iter().enumerate().map(|(idx, values)| {
                (
                    idx as PointOffsetType,
                    values
                        .iter()
                        .map(|value| N::into_referenced(N::from_owned(value))),
                )
            }),
        )?;

        {
            const BITS_IN_BYTE: usize = 8;
            let deleted_flags_count = point_to_values.len();
            let deleted_file = create_and_ensure_length(
                &deleted_path,
                BITS_IN_BYTE
                    * BITS_IN_BYTE
                    * deleted_flags_count.div_ceil(BITS_IN_BYTE * BITS_IN_BYTE),
            )?;
            let mut deleted_mmap = unsafe { MmapMut::map_mut(&deleted_file)? };
            deleted_mmap.fill(0);
            let mut deleted_bitflags = MmapBitSlice::from(deleted_mmap, 0);
            for (idx, values) in point_to_values.iter().enumerate() {
                if values.is_empty() {
                    deleted_bitflags.set(idx, true);
                }
            }
        }

        Self::load(path)
    }

    pub fn flusher(&self) -> Flusher {
        self.deleted.flusher()
    }

    pub fn clear(self) -> OperationResult<()> {
        let files = self.files();
        drop(self);
        for file in files {
            std::fs::remove_file(file)?;
        }
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(HASHMAP_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.point_to_values.files());
        files
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) {
        let idx = idx as usize;
        if idx < self.deleted.len() && !self.deleted.get(idx).unwrap_or(true) {
            self.deleted.set(idx, true);
            self.deleted_count += 1;
        }
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&N) -> bool) -> bool {
        if self.deleted.get(idx as usize).unwrap_or(true) {
            self.point_to_values
                .check_values_any(idx, |v| check_fn(N::from_referenced(&v)))
        } else {
            false
        }
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
    ) -> Option<Box<dyn Iterator<Item = N::Referenced<'_>> + '_>> {
        if !self.deleted.get(idx as usize).unwrap_or(true) {
            Some(Box::new(self.point_to_values.get_values(idx)?))
        } else {
            None
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        if !self.deleted.get(idx as usize).unwrap_or(true) {
            self.point_to_values.get_values_count(idx)
        } else {
            None
        }
    }

    pub fn get_indexed_points(&self) -> usize {
        self.point_to_values
            .len()
            .saturating_sub(self.deleted_count)
    }

    /// Returns the number of key-value pairs in the index.
    /// Note that is doesn't count deleted pairs.
    pub fn get_values_count(&self) -> usize {
        self.total_key_value_pairs
    }

    pub fn get_unique_values_count(&self) -> usize {
        self.value_to_points.keys_count()
    }

    pub fn get_count_for_value(&self, value: &N) -> Option<usize> {
        self.value_to_points
            .get(value)
            .ok()
            .flatten()
            .map(|p| p.len())
    }

    pub fn get_iterator(&self, value: &N) -> Box<dyn Iterator<Item = &PointOffsetType> + '_> {
        if let Some(slice) = self.value_to_points.get(value).ok().flatten() {
            Box::new(slice.iter())
        } else {
            Box::new(iter::empty())
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.value_to_points.keys())
    }

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (&N, usize)> + '_ {
        self.value_to_points.iter().map(|(k, v)| (k, v.len()))
    }

    pub fn iter_values_map(&self) -> impl Iterator<Item = (&N, IdRefIter<'_>)> + '_ {
        self.value_to_points.iter().map(|(k, v)| {
            (
                k,
                Box::new(v.iter()) as Box<dyn Iterator<Item = &PointOffsetType>>,
            )
        })
    }
}
