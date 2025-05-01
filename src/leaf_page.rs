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

// Metadata for each key-value pair
#[derive(Debug, Clone, Copy)]
pub struct LeafPageEntry {
    pub key: u64,
    pub value_offset: usize,
    pub value_length: usize,
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
pub const KEY_SIZE: usize = 8; // 8 bytes for u64 key
pub const VALUE_LENGTH_SIZE: usize = 8; // 8 bytes for value length
pub const METADATA_ENTRY_SIZE: usize = KEY_SIZE + VALUE_LENGTH_SIZE;

#[derive(Debug)]
pub struct LeafPage {
    pub page_type: PageType,
    pub page_size: usize,
    pub metadata: Vec<LeafPageEntry>,
    pub data: Vec<u8>,
    pub prev_page_id: u64,
    pub next_page_id: u64,
}

impl LeafPage {
    pub fn new(page_size: usize) -> Self {
        LeafPage {
            page_type: PageType::LeafPage,
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
        // Using HEADER_SIZE constant instead of magic numbers
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
            bytes.extend_from_slice(&meta.key.to_le_bytes());
            bytes.extend_from_slice(&(meta.value_length as u64).to_le_bytes());
        }

        // Write data
        bytes.extend_from_slice(&self.data);

        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        // Check if the bytes array is long enough for the header
        if bytes.len() < HEADER_SIZE {
            // Return an empty LeafPage if the bytes array is too short
            return LeafPage::new(bytes.len());
        }

        let mut offset = 0;

        // Read page type (1 byte)
        let page_type = PageType::from_u8(bytes[offset]).unwrap_or(PageType::LeafPage);
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
        let mut current_offset = 0;
        for _ in 0..count {
            // Check if there's enough data for the metadata entry
            if offset + 16 > bytes.len() {
                break;
            }

            let key = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let value_length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            metadata.push(LeafPageEntry {
                key,
                value_offset: current_offset,
                value_length,
            });
            current_offset += value_length;
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

        LeafPage {
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

    pub fn metadata(&self) -> &[LeafPageEntry] {
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

    // New method that takes a u64 key
    pub fn get(&self, key: u64) -> Option<&[u8]> {
        // Find the metadata for the key
        for meta in &self.metadata {
            if meta.key == key {
                return Some(&self.data[meta.value_offset..meta.value_offset + meta.value_length]);
            }
        }
        None
    }
    
    // New method that takes a u64 key
    pub fn insert(&mut self, key: u64, value: &[u8]) -> bool {
        // Check if key already exists
        if let Some(pos) = self.metadata.iter().position(|meta| meta.key == key) {
            // Key exists, update the value
            let old_meta = self.metadata[pos];
            let required_space = if value.len() > old_meta.value_length {
                // Only count the additional space needed
                value.len() - old_meta.value_length
            } else {
                0 // No additional space needed if new value is smaller
            };

            // Calculate total space after update
            let total_space = self.data.len() + required_space;

            // Check if we have enough space
            if total_space > self.page_size {
                return false; // Not enough space
            }

            // Remove old data and metadata
            self.metadata.remove(pos);
            self.compact_data();

            // Add new data and metadata
            let new_meta = LeafPageEntry {
                key,
                value_offset: self.data.len(),
                value_length: value.len(),
            };

            self.data.extend_from_slice(value);
            self.metadata.push(new_meta);

            return true;
        }

        // Calculate total space needed for new entry
        let required_space = value.len();
        let metadata_size = (self.metadata.len() + 1) * METADATA_ENTRY_SIZE;
        // Using HEADER_SIZE constant instead of magic numbers
        let total_space = self.data.len() + required_space + metadata_size + HEADER_SIZE;

        // Check if we have enough space
        if total_space > self.page_size {
            return false; // Not enough space
        }

        // Create new metadata
        let new_meta = LeafPageEntry {
            key,
            value_offset: self.data.len(),
            value_length: value.len(),
        };

        // Add the new data
        self.data.extend_from_slice(value);

        // Add the new metadata
        self.metadata.push(new_meta);

        true
    }
    
    // New method that takes a u64 key
    pub fn delete(&mut self, key: u64) -> bool {
        // Find and remove the metadata
        if let Some(pos) = self.metadata.iter().position(|meta| meta.key == key) {
            self.metadata.remove(pos);
            self.compact_data();
            true
        } else {
            false
        }
    }
    
    pub fn is_full(&self, value: &[u8]) -> bool {
        // Calculate space needed for new entry
        let new_metadata_size = std::mem::size_of::<LeafPageEntry>();
        let new_data_size = value.len();

        // Calculate current space used
        let current_metadata_size = self.metadata.len() * std::mem::size_of::<LeafPageEntry>();
        let current_data_size = self.data.len();

        // Using HEADER_SIZE constant instead of calculating it again

        // Check if we have enough space
        current_metadata_size + current_data_size + new_metadata_size + new_data_size + HEADER_SIZE > self.page_size
    }
    
    pub fn split(&mut self) -> Option<LeafPage> {
        if self.metadata.len() < 2 {
            return None;
        }

        // Sort metadata by key for consistent splitting
        self.metadata.sort_by(|a, b| a.key.cmp(&b.key));

        // Calculate split point
        let split_point = self.metadata.len() / 2;

        // Create new page with same size
        let mut new_page = LeafPage::new_with_size(self.page_size);
        new_page.page_type = PageType::LeafPage;

        // First pass: collect all data
        let mut all_data = Vec::new();
        for meta in &self.metadata {
            let key = meta.key;
            let value = self.data[meta.value_offset..meta.value_offset + meta.value_length].to_vec();
            all_data.push((key, value));
        }

        // Clear current data and metadata
        self.data.clear();
        self.metadata.clear();
        let mut new_data = Vec::new();
        let mut new_metadata = Vec::new();

        // Second pass: split data
        for (i, (key, value)) in all_data.into_iter().enumerate() {
            if i < split_point {
                // Keep in current page
                let new_meta = LeafPageEntry {
                    key,
                    value_offset: self.data.len(),
                    value_length: value.len(),
                };
                self.data.extend_from_slice(&value);
                self.metadata.push(new_meta);
            } else {
                // Move to new page
                let new_meta = LeafPageEntry {
                    key,
                    value_offset: new_data.len(),
                    value_length: value.len(),
                };
                new_data.extend_from_slice(&value);
                new_metadata.push(new_meta);
            }
        }

        // Update metadata
        new_page.data = new_data;
        new_page.metadata = new_metadata;

        Some(new_page)
    }

    pub fn new_with_size(page_size: usize) -> Self {
        LeafPage {
            page_type: PageType::LeafPage,
            data: Vec::new(),
            metadata: Vec::new(),
            page_size,
            prev_page_id: 0,
            next_page_id: 0,
        }
    }

    fn compact_data(&mut self) {
        if self.metadata.is_empty() {
            self.data.clear();
            return;
        }

        // Sort metadata by key
        self.metadata.sort_by_key(|m| m.key);

        // Rebuild data
        let mut new_data = Vec::new();
        let mut new_metadata = Vec::new();

        for meta in &self.metadata {
            let value = &self.data[meta.value_offset..meta.value_offset + meta.value_length];

            let new_meta = LeafPageEntry {
                key: meta.key,
                value_offset: new_data.len(),
                value_length: meta.value_length,
            };

            new_data.extend_from_slice(value);
            new_metadata.push(new_meta);
        }

        self.data = new_data;
        self.metadata = new_metadata;
    }

    pub fn max_value_size(&self) -> usize {
        // Reserve space for metadata
        let metadata_overhead = 32; // 16 bytes for metadata entry + buffer
        self.page_size - metadata_overhead
    }

    pub fn is_value_too_large(&self, value: &[u8]) -> bool {
        value.len() > self.max_value_size()
    }

    pub fn get_value_chunk_size(&self) -> usize {
        self.max_value_size()
    }
}

