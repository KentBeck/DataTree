use std::error::Error;
use std::fmt;
use crate::data_tree::PageType;

// Define a custom error type for when a key is not found
#[derive(Debug)]
pub struct KeyNotFoundError;

impl fmt::Display for KeyNotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Key not found in page")
    }
}

impl Error for KeyNotFoundError {}

// Metadata for each run of key-value pairs with identical values
#[derive(Debug, Clone, Copy)]
pub struct RleLeafPageEntry {
    pub start_key: u64,     // First key in the run
    pub end_key: u64,       // Last key in the run (inclusive)
    pub value_offset: usize, // Offset of the value in the data array
    pub value_length: usize, // Length of the value
}

// Constants for page header sizes
pub const PAGE_TYPE_SIZE: usize = 1; // 1 byte for page type
pub const COUNT_SIZE: usize = 8;     // 8 bytes for metadata count
pub const DATA_START_SIZE: usize = 8; // 8 bytes for data start offset
pub const USED_BYTES_SIZE: usize = 8; // 8 bytes for used bytes
pub const PREV_PAGE_ID_SIZE: usize = 8; // 8 bytes for previous page ID
pub const NEXT_PAGE_ID_SIZE: usize = 8; // 8 bytes for next page ID
pub const HEADER_SIZE: usize = PAGE_TYPE_SIZE + COUNT_SIZE + DATA_START_SIZE +
                              USED_BYTES_SIZE + PREV_PAGE_ID_SIZE + NEXT_PAGE_ID_SIZE;

// Constants for metadata entry sizes
pub const START_KEY_SIZE: usize = 8; // 8 bytes for u64 start key
pub const END_KEY_SIZE: usize = 8;   // 8 bytes for u64 end key
pub const VALUE_OFFSET_SIZE: usize = 8; // 8 bytes for value offset
pub const VALUE_LENGTH_SIZE: usize = 8; // 8 bytes for value length
pub const METADATA_ENTRY_SIZE: usize = START_KEY_SIZE + END_KEY_SIZE + VALUE_OFFSET_SIZE + VALUE_LENGTH_SIZE;

#[derive(Debug)]
pub struct RleLeafPage {
    pub page_type: PageType,
    pub page_size: usize,
    pub metadata: Vec<RleLeafPageEntry>,
    pub data: Vec<u8>,
    pub prev_page_id: u64,
    pub next_page_id: u64,
}

impl RleLeafPage {
    pub fn new(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            return Self::new_empty(bytes.len());
        }
        Self::deserialize(bytes)
    }

    pub fn new_empty(page_size: usize) -> Self {
        RleLeafPage {
            page_type: PageType::RleLeafPage,
            page_size,
            metadata: Vec::new(),
            data: Vec::new(),
            prev_page_id: 0,
            next_page_id: 0,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.page_size);

        // Write page type (1 byte)
        bytes.push(self.page_type.to_u8());

        // Write metadata count (8 bytes)
        bytes.extend_from_slice(&(self.metadata.len() as u64).to_le_bytes());

        // Calculate data start offset
        let metadata_size = self.metadata.len() * METADATA_ENTRY_SIZE;
        let data_start = HEADER_SIZE + metadata_size;

        // Write data start offset (8 bytes)
        bytes.extend_from_slice(&(data_start as u64).to_le_bytes());

        // Write used bytes (8 bytes)
        bytes.extend_from_slice(&(self.data.len() as u64).to_le_bytes());

        // Write prev_page_id (8 bytes)
        bytes.extend_from_slice(&self.prev_page_id.to_le_bytes());

        // Write next_page_id (8 bytes)
        bytes.extend_from_slice(&self.next_page_id.to_le_bytes());

        // Write metadata entries
        for meta in &self.metadata {
            bytes.extend_from_slice(&meta.start_key.to_le_bytes());
            bytes.extend_from_slice(&meta.end_key.to_le_bytes());
            bytes.extend_from_slice(&(meta.value_offset as u64).to_le_bytes());
            bytes.extend_from_slice(&(meta.value_length as u64).to_le_bytes());
        }

        // Write data
        bytes.extend_from_slice(&self.data);

        // Pad with zeros if necessary to reach page_size
        if bytes.len() < self.page_size {
            bytes.resize(self.page_size, 0);
        }

        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        // Check if the bytes array is long enough for the header
        if bytes.len() < HEADER_SIZE {
            // Return an empty RleLeafPage if the bytes array is too short
            return RleLeafPage::new_empty(bytes.len());
        }

        let mut offset = 0;

        // Read page type (1 byte)
        let page_type = PageType::from_u8(bytes[offset]).unwrap_or(PageType::RleLeafPage);
        offset += 1;

        // Read metadata count (8 bytes)
        let count = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read data start offset (8 bytes)
        let data_start = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read used bytes (8 bytes)
        let used_bytes = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read prev_page_id (8 bytes)
        let prev_page_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read next_page_id (8 bytes)
        let next_page_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read metadata entries
        let mut metadata = Vec::with_capacity(count as usize);
        for _ in 0..count {
            // Check if there's enough data for the metadata entry
            if offset + METADATA_ENTRY_SIZE > bytes.len() {
                break;
            }

            let start_key = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let end_key = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let value_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            let value_length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;

            metadata.push(RleLeafPageEntry {
                start_key,
                end_key,
                value_offset,
                value_length,
            });
        }

        // Read data
        let data_start_usize = data_start as usize;
        let used_bytes_usize = used_bytes as usize;
        let data = if data_start_usize < bytes.len() {
            let end = std::cmp::min(data_start_usize + used_bytes_usize, bytes.len());
            bytes[data_start_usize..end].to_vec()
        } else {
            Vec::new()
        };

        RleLeafPage {
            page_type,
            page_size: bytes.len(),
            metadata,
            data,
            prev_page_id,
            next_page_id,
        }
    }

    pub fn page_type(&self) -> PageType {
        self.page_type
    }

    pub fn metadata(&self) -> &[RleLeafPageEntry] {
        &self.metadata
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn prev_page_id(&self) -> u64 {
        self.prev_page_id
    }

    pub fn next_page_id(&self) -> u64 {
        self.next_page_id
    }

    pub fn set_prev_page_id(&mut self, page_id: u64) {
        self.prev_page_id = page_id;
    }

    pub fn set_next_page_id(&mut self, page_id: u64) {
        self.next_page_id = page_id;
    }

    // Get a value for a specific key
    pub fn get(&self, key: u64) -> Option<&[u8]> {
        // Find the metadata entry that contains the key
        for meta in &self.metadata {
            if key >= meta.start_key && key <= meta.end_key {
                return Some(&self.data[meta.value_offset..meta.value_offset + meta.value_length]);
            }
        }
        None
    }

    // Insert a key-value pair
    pub fn put(&mut self, key: u64, value: &[u8]) -> bool {
        // First, check if we need to update an existing run
        let mut found_run = false;
        let mut run_index = 0;
        let mut adjacent_run_before = None;
        let mut adjacent_run_after = None;

        // Check if the key fits into an existing run or is adjacent to one
        for (i, meta) in self.metadata.iter().enumerate() {
            // Check if key is within an existing run
            if key >= meta.start_key && key <= meta.end_key {
                found_run = true;
                run_index = i;
                break;
            }

            // Check if key is adjacent to the start of a run
            if key + 1 == meta.start_key {
                adjacent_run_after = Some(i);
            }

            // Check if key is adjacent to the end of a run
            if key == meta.end_key + 1 {
                adjacent_run_before = Some(i);
            }
        }

        // If the key is in an existing run, check if the value matches
        if found_run {
            let meta = &self.metadata[run_index];
            let current_value = &self.data[meta.value_offset..meta.value_offset + meta.value_length];

            if current_value == value {
                // Value already matches, nothing to do
                return true;
            } else {
                // Value doesn't match, need to split the run
                return self.split_run_and_insert(run_index, key, value);
            }
        }

        // Check if we can extend an existing run
        if let Some(run_idx) = adjacent_run_before {
            let meta = &self.metadata[run_idx];
            let current_value = &self.data[meta.value_offset..meta.value_offset + meta.value_length];

            if current_value == value {
                // Can extend the run to include this key
                self.metadata[run_idx].end_key = key;

                // Check if we can merge with the next run
                if let Some(next_idx) = adjacent_run_after {
                    let next_meta = &self.metadata[next_idx];
                    let next_value = &self.data[next_meta.value_offset..next_meta.value_offset + next_meta.value_length];

                    if next_value == value {
                        // Merge the runs
                        let end_key = next_meta.end_key;
                        self.metadata[run_idx].end_key = end_key;
                        self.metadata.remove(next_idx);
                    }
                }

                return true;
            }
        }

        if let Some(run_idx) = adjacent_run_after {
            let meta = &self.metadata[run_idx];
            let current_value = &self.data[meta.value_offset..meta.value_offset + meta.value_length];

            if current_value == value {
                // Can extend the run to include this key
                self.metadata[run_idx].start_key = key;
                return true;
            }
        }

        // Need to create a new run
        // Check if the value already exists in the data
        let mut value_offset = 0;
        let mut value_exists = false;

        for meta in &self.metadata {
            if meta.value_length == value.len() {
                let existing_value = &self.data[meta.value_offset..meta.value_offset + meta.value_length];
                if existing_value == value {
                    value_offset = meta.value_offset;
                    value_exists = true;
                    break;
                }
            }
        }

        // Calculate required space
        let required_space = if !value_exists {
            value.len() // Need to store the value
        } else {
            0 // Value already exists
        };

        // Calculate total space needed
        let metadata_size = (self.metadata.len() + 1) * METADATA_ENTRY_SIZE;
        let total_space = HEADER_SIZE + metadata_size + self.data.len() + required_space;

        // Check if we have enough space
        if total_space > self.page_size {
            return false; // Not enough space
        }

        // Add the value to data if it doesn't exist
        if !value_exists {
            value_offset = self.data.len();
            self.data.extend_from_slice(value);
        }

        // Create new metadata entry
        let new_meta = RleLeafPageEntry {
            start_key: key,
            end_key: key,
            value_offset,
            value_length: value.len(),
        };

        // Add the new metadata
        self.metadata.push(new_meta);

        // Sort metadata by start_key for easier lookups
        self.metadata.sort_by_key(|m| m.start_key);

        true
    }

    // Helper method to split a run and insert a new value
    fn split_run_and_insert(&mut self, run_index: usize, key: u64, value: &[u8]) -> bool {
        let meta = self.metadata[run_index];

        // Check if we need to split into 1 or 2 new runs
        let split_into_two = key > meta.start_key && key < meta.end_key;

        // Calculate required space
        let new_metadata_entries = if split_into_two { 2 } else { 1 };
        let _required_metadata_space = new_metadata_entries * METADATA_ENTRY_SIZE;

        // Check if the value already exists
        let mut value_offset = 0;
        let mut value_exists = false;

        for m in &self.metadata {
            if m.value_length == value.len() {
                let existing_value = &self.data[m.value_offset..m.value_offset + m.value_length];
                if existing_value == value {
                    value_offset = m.value_offset;
                    value_exists = true;
                    break;
                }
            }
        }

        // Calculate required data space
        let required_data_space = if !value_exists {
            value.len()
        } else {
            0
        };

        // Calculate total space needed
        let total_space = HEADER_SIZE +
                         (self.metadata.len() + new_metadata_entries) * METADATA_ENTRY_SIZE +
                         self.data.len() + required_data_space;

        // Check if we have enough space
        if total_space > self.page_size {
            return false; // Not enough space
        }

        // Add the value to data if it doesn't exist
        if !value_exists {
            value_offset = self.data.len();
            self.data.extend_from_slice(value);
        }

        // Remove the original run
        self.metadata.remove(run_index);

        // Create new runs
        if key == meta.start_key {
            // Key is at the start of the run
            // Create a new run for the key
            let new_meta1 = RleLeafPageEntry {
                start_key: key,
                end_key: key,
                value_offset,
                value_length: value.len(),
            };

            // Create a run for the rest of the original run
            if meta.end_key > key {
                let new_meta2 = RleLeafPageEntry {
                    start_key: key + 1,
                    end_key: meta.end_key,
                    value_offset: meta.value_offset,
                    value_length: meta.value_length,
                };
                self.metadata.push(new_meta2);
            }

            self.metadata.push(new_meta1);
        } else if key == meta.end_key {
            // Key is at the end of the run
            // Create a run for the original run except the last key
            let new_meta1 = RleLeafPageEntry {
                start_key: meta.start_key,
                end_key: key - 1,
                value_offset: meta.value_offset,
                value_length: meta.value_length,
            };

            // Create a new run for the key
            let new_meta2 = RleLeafPageEntry {
                start_key: key,
                end_key: key,
                value_offset,
                value_length: value.len(),
            };

            self.metadata.push(new_meta1);
            self.metadata.push(new_meta2);
        } else {
            // Key is in the middle of the run
            // Create a run for the part before the key
            let new_meta1 = RleLeafPageEntry {
                start_key: meta.start_key,
                end_key: key - 1,
                value_offset: meta.value_offset,
                value_length: meta.value_length,
            };

            // Create a new run for the key
            let new_meta2 = RleLeafPageEntry {
                start_key: key,
                end_key: key,
                value_offset,
                value_length: value.len(),
            };

            // Create a run for the part after the key
            let new_meta3 = RleLeafPageEntry {
                start_key: key + 1,
                end_key: meta.end_key,
                value_offset: meta.value_offset,
                value_length: meta.value_length,
            };

            self.metadata.push(new_meta1);
            self.metadata.push(new_meta2);
            self.metadata.push(new_meta3);
        }

        // Sort metadata by start_key
        self.metadata.sort_by_key(|m| m.start_key);

        true
    }

    // Delete a key
    pub fn delete(&mut self, key: u64) -> bool {
        // Find the run containing the key
        let mut found_run = false;
        let mut run_index = 0;

        for (i, meta) in self.metadata.iter().enumerate() {
            if key >= meta.start_key && key <= meta.end_key {
                found_run = true;
                run_index = i;
                break;
            }
        }

        if !found_run {
            return false; // Key not found
        }

        let meta = self.metadata[run_index];

        // Handle different cases
        if meta.start_key == key && meta.end_key == key {
            // Single key run, just remove it
            self.metadata.remove(run_index);
        } else if meta.start_key == key {
            // Key is at the start of the run
            self.metadata[run_index].start_key = key + 1;
        } else if meta.end_key == key {
            // Key is at the end of the run
            self.metadata[run_index].end_key = key - 1;
        } else {
            // Key is in the middle of the run, need to split
            let new_meta = RleLeafPageEntry {
                start_key: key + 1,
                end_key: meta.end_key,
                value_offset: meta.value_offset,
                value_length: meta.value_length,
            };

            // Update the original run to end before the key
            self.metadata[run_index].end_key = key - 1;

            // Add the new run
            self.metadata.push(new_meta);

            // Sort metadata by start_key
            self.metadata.sort_by_key(|m| m.start_key);
        }

        // Clean up unused data if needed
        self.compact_data_if_needed();

        true
    }

    // Check if the page is full for a new key-value pair
    pub fn is_full(&self, value: &[u8]) -> bool {
        // Calculate space needed for new entry (worst case: new metadata + new value)
        let new_metadata_size = METADATA_ENTRY_SIZE;
        let new_data_size = value.len(); // Worst case: value doesn't exist yet

        // Calculate current space used
        let current_metadata_size = self.metadata.len() * METADATA_ENTRY_SIZE;
        let current_data_size = self.data.len();

        // Check if we have enough space
        let total_required = HEADER_SIZE + current_metadata_size + current_data_size + new_metadata_size + new_data_size;

        // Add some buffer to ensure we don't get too close to the limit
        let with_buffer = total_required + 16; // 16 bytes buffer

        with_buffer > self.page_size
    }

    // Split the page into two
    pub fn split(&mut self) -> Option<RleLeafPage> {
        if self.metadata.len() < 2 {
            return None;
        }

        // Sort metadata by key for consistent splitting
        self.metadata.sort_by(|a, b| a.start_key.cmp(&b.start_key));

        // Calculate split point
        let split_point = self.metadata.len() / 2;

        // Create new page with same size
        let mut new_page = RleLeafPage::new_empty(self.page_size);
        new_page.page_type = PageType::RleLeafPage;

        // Move metadata entries to the new page
        let entries_to_move = self.metadata.split_off(split_point);

        // Collect all unique values from the moved entries
        let mut values_to_move = Vec::new();
        for meta in &entries_to_move {
            let value = &self.data[meta.value_offset..meta.value_offset + meta.value_length];
            // Check if this value is already in values_to_move
            if !values_to_move.iter().any(|(_, v)| v == value) {
                values_to_move.push((meta.value_offset, value.to_vec()));
            }
        }

        // Add values to the new page and update metadata
        for meta in &mut new_page.metadata {
            // Find the corresponding value
            for (old_offset, value) in &values_to_move {
                if meta.value_offset == *old_offset {
                    // Update the offset to point to the new location
                    meta.value_offset = new_page.data.len();
                    new_page.data.extend_from_slice(value);
                    break;
                }
            }
        }

        // Set the metadata for the new page
        new_page.metadata = entries_to_move;

        // Update the offsets in the new page's metadata
        let mut offset_map = std::collections::HashMap::new();

        for (old_offset, value) in &values_to_move {
            let new_offset = new_page.data.len();
            offset_map.insert(*old_offset, new_offset);
            new_page.data.extend_from_slice(value);
        }

        for meta in &mut new_page.metadata {
            if let Some(&new_offset) = offset_map.get(&meta.value_offset) {
                meta.value_offset = new_offset;
            }
        }

        // Compact the current page's data
        self.compact_data();

        Some(new_page)
    }

    // Compact the data to remove unused values
    pub fn compact_data(&mut self) {
        if self.metadata.is_empty() {
            self.data.clear();
            return;
        }

        // Collect all unique values that are still in use
        let mut used_values = Vec::new();
        for meta in &self.metadata {
            let value = &self.data[meta.value_offset..meta.value_offset + meta.value_length];
            // Check if this value is already in used_values
            if !used_values.iter().any(|(_, v)| v == value) {
                used_values.push((meta.value_offset, value.to_vec()));
            }
        }

        // Create new data array
        let mut new_data = Vec::new();
        let mut offset_map = std::collections::HashMap::new();

        for (old_offset, value) in used_values {
            let new_offset = new_data.len();
            offset_map.insert(old_offset, new_offset);
            new_data.extend_from_slice(&value);
        }

        // Update metadata with new offsets
        for meta in &mut self.metadata {
            if let Some(&new_offset) = offset_map.get(&meta.value_offset) {
                meta.value_offset = new_offset;
            }
        }

        // Replace data with compacted version
        self.data = new_data;
    }

    // Only compact data if there's a significant amount of unused data
    fn compact_data_if_needed(&mut self) {
        // Count how many unique values are still in use
        let mut used_offsets = std::collections::HashSet::new();
        for meta in &self.metadata {
            used_offsets.insert(meta.value_offset);
        }

        // Calculate total size of used values
        let mut used_size = 0;
        for meta in &self.metadata {
            if used_offsets.contains(&meta.value_offset) {
                used_size += meta.value_length;
                used_offsets.remove(&meta.value_offset);
            }
        }

        // If less than 75% of data is used, compact
        if used_size < self.data.len() * 3 / 4 {
            self.compact_data();
        }
    }

    pub fn max_value_size(&self) -> usize {
        // Reserve space for metadata and header
        let metadata_overhead = METADATA_ENTRY_SIZE + 32; // Buffer for safety
        self.page_size - HEADER_SIZE - metadata_overhead
    }

    pub fn is_value_too_large(&self, value: &[u8]) -> bool {
        value.len() > self.max_value_size()
    }

    pub fn get_value_chunk_size(&self) -> usize {
        self.max_value_size()
    }
}
