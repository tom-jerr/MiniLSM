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

use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
    /// Whether reverse iteration
    prev: bool,
}

impl Block {
    // we always need first key to compute overlap and combine a entrie key
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        KeyVec::from_vec(key.to_vec())
    }
}
impl BlockIterator {
    fn new(block: Arc<Block>, prev: bool) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            prev,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block, false);
        iter.seek_to_first();
        iter
    }

    pub fn create_and_seek_to_last(block: Arc<Block>) -> Self {
        let offset_len = block.offsets.len();
        let mut iter = Self::new(block, true);
        // seek to the last entry in the block
        if offset_len > 0 {
            let last_idx = offset_len - 1;
            iter.seek_to(last_idx);
        } else {
            // if there is no entry, just return an invalid iterator
            iter.key.clear();
            iter.value_range = (0, 0);
        }
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block, false);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        let start = self.value_range.0;
        let end = self.value_range.1;
        &self.block.data[start..end]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    pub fn seek_to_last(&mut self) {
        let offset_len = self.block.offsets.len();
        if offset_len == 0 {
            // if there is no entry, just return an invalid iterator
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let last_idx = offset_len - 1;
        self.seek_to(last_idx);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.prev {
            println!("Before next, idx: {}, prev: true", self.idx);
            if self.idx == 0 {
                // if we are at the first element, we can't go back anymore
                self.key.clear();
                self.value_range = (0, 0);
                return;
            }
            self.idx -= 1;
            println!("After next, idx: {}, prev: true", self.idx);
            self.seek_to(self.idx);
            return;
        }
        self.idx += 1;
        self.seek_to(self.idx);
    }
    /// Seeks to the idx-th key in the block.
    fn seek_to(&mut self, idx: usize) {
        // reach the end of block, return false
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        // Since `get_u16()` will automatically move the ptr 2 bytes ahead here,
        // we don't need to manually advance it
        let overlap_len = entry.get_u16() as usize;
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        self.key.clear();
        // combine overlap first key and unique key
        self.key.append(&self.first_key.raw_ref()[..overlap_len]);
        self.key.append(key);

        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + SIZEOF_U16/* overlap len */ + SIZEOF_U16/* key len */ + key_len + SIZEOF_U16/* value len */;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = (low + high) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return,
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        self.seek_to(low);
    }
}
