// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::Bytes;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;

use ouroboros::self_referencing;
// use serde::de::value;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
#[derive(Debug)]
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// 将 Rust 范围转换为 (lower, upper) 的 Bound<Bytes>
pub trait ToBounds {
    fn to_bounds(&self) -> (Bound<Bytes>, Bound<Bytes>);
}

// 为 RangeFull（即 `..`）实现
impl ToBounds for std::ops::RangeFull {
    fn to_bounds(&self) -> (Bound<Bytes>, Bound<Bytes>) {
        (Bound::Unbounded, Bound::Unbounded)
    }
}

// 为 RangeInclusive<&[u8; N]> 实现，闭区间[start, end]
impl<const N: usize> ToBounds for std::ops::RangeInclusive<&[u8; N]> {
    fn to_bounds(&self) -> (Bound<Bytes>, Bound<Bytes>) {
        let (start, end) = (self.start().as_slice(), self.end().as_slice());
        (
            Bound::Included(Bytes::copy_from_slice(start)),
            Bound::Included(Bytes::copy_from_slice(end)),
        )
    }
}

// 为 Range<&[u8; N]> 实现，半开区间[start, end)
impl<const N: usize> ToBounds for std::ops::Range<&[u8; N]> {
    fn to_bounds(&self) -> (Bound<Bytes>, Bound<Bytes>) {
        let (start, end) = (self.start.as_slice(), self.end.as_slice());
        (
            Bound::Included(Bytes::copy_from_slice(start)),
            Bound::Excluded(Bytes::copy_from_slice(end)),
        )
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        // Create a new skipmap for the mem-table.
        let map = Arc::new(SkipMap::new());
        let approximate_size = Arc::new(AtomicUsize::new(0));

        MemTable {
            map,
            wal: None,
            id: _id,
            approximate_size,
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    pub fn for_testing_scan_range_slice<T: ToBounds>(&self, range: T) -> MemTableIterator {
        self.scan_range(range)
    }

    /// Get a value by key.
    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        let key_bytes = Bytes::copy_from_slice(_key); // Convert the key slice to `Bytes`
        // Use the skipmap to get the value by key.
        self.map
            .get(key_bytes.as_ref())
            .map(|value| value.clone().value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let key_bytes = Bytes::copy_from_slice(_key); // Convert the key slice to `Bytes`
        let value_bytes = Bytes::copy_from_slice(_value); // Convert the value slice to `Bytes`
        // Insert the key-value pair into the skipmap.
        let ret = self.map.insert(key_bytes, value_bytes);
        if ret.key() == _key {
            // If the key was not present, increment the approximate size.
            self.approximate_size.fetch_add(
                _key.len() + _value.len(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        Result::Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> MemTableIterator {
        let (lower_bound, upper_bound) = (map_bound(_lower), map_bound(_upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(), // Pass the skipmap
            iter_builder: |map| map.range((lower_bound, upper_bound)),
            item: (Bytes::new(), Bytes::new()), // Initialize with empty Bytes for the first entry
        }
        .build();
        iter.next().unwrap();
        iter
    }

    pub fn scan_range<R: ToBounds>(&self, range: R) -> MemTableIterator {
        let (lower_bound, upper_bound) = range.to_bounds();
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(), // Pass the skipmap
            iter_builder: |map| map.range((lower_bound, upper_bound)),
            item: (Bytes::new(), Bytes::new()), // Initialize with empty Bytes for the first entry
        }
        .build();
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        unimplemented!()
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}
impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item()
            .1 // Get the value from the tuple (Bytes, Bytes)
            .as_ref() // Convert to &[u8] 
    }

    fn key(&self) -> KeySlice {
        KeySlice::from_slice(
            self.borrow_item()
                .0 // Get the key from the tuple (Bytes, Bytes)
                .as_ref(), // Convert to &[u8]
        )
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty() // Ensure the key is not empty
    }

    fn next(&mut self) -> Result<()> {
        // Move to the next entry in the skipmap iterator.
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        // Update the current item in the MemTableIterator.
        self.with_mut(|x| {
            *x.item = entry; // Update the item with the new key-value pair
        });

        Ok(())
    }
}
