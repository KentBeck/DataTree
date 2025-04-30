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
pub struct KeyValueMeta {
    pub key_offset: usize,
    pub key_length: usize,
    pub value_offset: usize,
    pub value_length: usize,
}



#[derive(Debug)]
pub struct LeafPage {
    pub page_type: PageType,
    pub page_size: usize,
    pub metadata: Vec<KeyValueMeta>,
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
        let header_size = 1 + 8 + 8 + 8 + 8 + 8; // page_type + count + data_start + used_bytes + prev_page_id + next_page_id
        let metadata_size = self.metadata.len() * 16; // 8 bytes for key_length + 8 bytes for value_length
        let data_start = header_size + metadata_size;

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
            bytes.extend_from_slice(&(meta.key_length as u64).to_le_bytes());
            bytes.extend_from_slice(&(meta.value_length as u64).to_le_bytes());
        }

        // Write data
        bytes.extend_from_slice(&self.data);

        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
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
            let key_length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            let value_length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            metadata.push(KeyValueMeta {
                key_offset: current_offset,
                key_length,
                value_offset: current_offset + key_length,
                value_length,
            });
            current_offset += key_length + value_length;
        }

        // Read data
        let data = bytes[data_start as usize..data_start as usize + used_bytes as usize].to_vec();

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

    pub fn metadata(&self) -> &[KeyValueMeta] {
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

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        // Find the metadata for the key
        for meta in &self.metadata {
            let meta_key = &self.data[meta.key_offset..meta.key_offset + meta.key_length];
            if meta_key == key {
                return Some(&self.data[meta.value_offset..meta.value_offset + meta.value_length]);
            }
        }
        None
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
        // Check if key already exists
        if let Some(pos) = self.metadata.iter().position(|meta| {
            let meta_key = &self.data[meta.key_offset..meta.key_offset + meta.key_length];
            meta_key == key
        }) {
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
            let new_meta = KeyValueMeta {
                key_offset: self.data.len(),
                key_length: key.len(),
                value_offset: self.data.len() + key.len(),
                value_length: value.len(),
            };

            self.data.extend_from_slice(key);
            self.data.extend_from_slice(value);
            self.metadata.push(new_meta);

            return true;
        }

        // Calculate total space needed for new entry
        let required_space = key.len() + value.len();
        let metadata_size = (self.metadata.len() + 1) * 16; // 8 bytes for key_length + 8 bytes for value_length
        let header_size = 1 + 8 + 8 + 8 + 8 + 8; // page_type + count + data_start + used_bytes + prev_page_id + next_page_id
        let total_space = self.data.len() + required_space + metadata_size + header_size;

        // Check if we have enough space
        if total_space > self.page_size {
            return false; // Not enough space
        }

        // Create new metadata
        let new_meta = KeyValueMeta {
            key_offset: self.data.len(),
            key_length: key.len(),
            value_offset: self.data.len() + key.len(),
            value_length: value.len(),
        };

        // Add the new data
        self.data.extend_from_slice(key);
        self.data.extend_from_slice(value);

        // Add the new metadata
        self.metadata.push(new_meta);

        true
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        // Find and remove the metadata
        if let Some(pos) = self.metadata.iter().position(|meta| {
            let meta_key = &self.data[meta.key_offset..meta.key_offset + meta.key_length];
            meta_key == key
        }) {
            self.metadata.remove(pos);
            self.compact_data();
            true
        } else {
            false
        }
    }

    pub fn is_full(&self, key: &[u8], value: &[u8]) -> bool {
        // Calculate space needed for new entry
        let new_metadata_size = std::mem::size_of::<KeyValueMeta>();
        let new_data_size = key.len() + value.len();

        // Calculate current space used
        let current_metadata_size = self.metadata.len() * std::mem::size_of::<KeyValueMeta>();
        let current_data_size = self.data.len();

        // Add header size (metadata count, data start, used bytes)
        let header_size = 3 * std::mem::size_of::<u64>();

        // Check if we have enough space
        current_metadata_size + current_data_size + new_metadata_size + new_data_size + header_size > self.page_size
    }

    pub fn split(&mut self) -> Option<LeafPage> {
        if self.metadata.len() < 2 {
            return None;
        }

        // Sort metadata by key for consistent splitting
        self.metadata.sort_by(|a, b| {
            let a_key = &self.data[a.key_offset..a.key_offset + a.key_length];
            let b_key = &self.data[b.key_offset..b.key_offset + b.key_length];
            a_key.cmp(b_key)
        });

        // Calculate split point
        let split_point = self.metadata.len() / 2;

        // Create new page with same size
        let mut new_page = LeafPage::new_with_size(self.page_size);
        new_page.page_type = PageType::LeafPage;

        // First pass: collect all data
        let mut all_data = Vec::new();
        for meta in &self.metadata {
            let key = self.data[meta.key_offset..meta.key_offset + meta.key_length].to_vec();
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
                let new_meta = KeyValueMeta {
                    key_offset: self.data.len(),
                    key_length: key.len(),
                    value_offset: self.data.len() + key.len(),
                    value_length: value.len(),
                };
                self.data.extend_from_slice(&key);
                self.data.extend_from_slice(&value);
                self.metadata.push(new_meta);
            } else {
                // Move to new page
                let new_meta = KeyValueMeta {
                    key_offset: new_data.len(),
                    key_length: key.len(),
                    value_offset: new_data.len() + key.len(),
                    value_length: value.len(),
                };
                new_data.extend_from_slice(&key);
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

        // Sort metadata by key_offset
        self.metadata.sort_by_key(|m| m.key_offset);

        // Rebuild data
        let mut new_data = Vec::new();
        let mut new_metadata = Vec::new();

        for meta in &self.metadata {
            let key = &self.data[meta.key_offset..meta.key_offset + meta.key_length];
            let value = &self.data[meta.value_offset..meta.value_offset + meta.value_length];

            let new_meta = KeyValueMeta {
                key_offset: new_data.len(),
                key_length: meta.key_length,
                value_offset: new_data.len() + meta.key_length,
                value_length: meta.value_length,
            };

            new_data.extend_from_slice(key);
            new_data.extend_from_slice(value);
            new_metadata.push(new_meta);
        }

        self.data = new_data;
        self.metadata = new_metadata;
    }

    pub fn max_value_size(&self) -> usize {
        // Reserve space for metadata and key
        let metadata_overhead = 32; // 16 bytes for metadata entry + buffer
        let key_overhead = 32; // Reasonable buffer for key size
        self.page_size - metadata_overhead - key_overhead
    }

    pub fn is_value_too_large(&self, value: &[u8]) -> bool {
        value.len() > self.max_value_size()
    }

    pub fn get_value_chunk_size(&self) -> usize {
        self.max_value_size()
    }
}

