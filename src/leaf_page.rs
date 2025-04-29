use std::error::Error;
use std::fmt;

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
struct KeyValueMeta {
    key: u64,
    offset: usize,
    length: usize,
}

#[derive(Debug)]
pub struct LeafPage {
    page_size: usize,
    metadata: Vec<KeyValueMeta>,
    data: Vec<u8>,
    prev_page_id: u64,
    next_page_id: u64,
}

impl LeafPage {
    pub fn new(page_size: usize) -> Self {
        LeafPage {
            page_size,
            metadata: Vec::new(),
            data: Vec::new(),
            prev_page_id: 0,
            next_page_id: 0,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.page_size);
        
        // Write metadata count (8 bytes)
        bytes.extend_from_slice(&(self.metadata.len() as u64).to_le_bytes());
        
        // Write data start offset (8 bytes)
        bytes.extend_from_slice(&(self.data.len() as u64).to_le_bytes());
        
        // Write used bytes (8 bytes)
        bytes.extend_from_slice(&(self.data.len() as u64).to_le_bytes());
        
        // Write prev_page_id (8 bytes)
        bytes.extend_from_slice(&self.prev_page_id.to_le_bytes());
        
        // Write next_page_id (8 bytes)
        bytes.extend_from_slice(&self.next_page_id.to_le_bytes());
        
        // Write metadata entries
        for meta in &self.metadata {
            bytes.extend_from_slice(&meta.key.to_le_bytes());
            bytes.extend_from_slice(&meta.offset.to_le_bytes());
            bytes.extend_from_slice(&meta.length.to_le_bytes());
        }
        
        // Write data
        bytes.extend_from_slice(&self.data);
        
        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        let mut offset = 0;
        
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
            let key = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let meta_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            metadata.push(KeyValueMeta { key, offset: meta_offset, length });
        }
        
        // Read data
        let data = bytes[offset..offset + used_bytes as usize].to_vec();
        
        LeafPage {
            page_size: bytes.len(),
            metadata,
            data,
            prev_page_id,
            next_page_id,
        }
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

    pub fn get(&self, key: u64) -> Result<&[u8], KeyNotFoundError> {
        // Find the metadata for the key
        let meta = self.metadata.iter()
            .find(|m| m.key == key)
            .ok_or(KeyNotFoundError)?;

        // Return the slice of data
        Ok(&self.data[meta.offset..meta.offset + meta.length])
    }

    // Helper method to calculate metadata size
    fn metadata_size(&self, is_update: bool) -> usize {
        let entries = if is_update {
            self.metadata.len()
        } else {
            self.metadata.len() + 1
        };
        entries * std::mem::size_of::<KeyValueMeta>()
    }

    // Helper method to calculate total space needed
    fn total_space_needed(&self, key: u64, data_size: usize, is_update: bool) -> usize {
        let metadata_size = self.metadata_size(is_update);
        let data_size = if is_update {
            // For updates, we only count the additional space needed
            if let Some(existing_meta) = self.metadata.iter().find(|m| m.key == key) {
                if data_size <= existing_meta.length {
                    self.data.len() // No additional space needed
                } else {
                    self.data.len() + (data_size - existing_meta.length)
                }
            } else {
                self.data.len() + data_size
            }
        } else {
            self.data.len() + data_size
        };

        metadata_size + data_size
    }

    // Helper method to check if we can add more data
    fn can_add(&self, key: u64, data_size: usize, is_update: bool) -> bool {
        self.total_space_needed(key, data_size, is_update) <= self.page_size
    }

    // Method to add data
    pub fn insert(&mut self, key: u64, value: &[u8]) -> Result<(), &'static str> {
        // Check if key already exists
        if let Some(index) = self.metadata.iter().position(|m| m.key == key) {
            // This is an update
            let old_meta = self.metadata[index];
            
            // Check if we have enough space (considering we'll reuse the old space)
            if value.len() <= old_meta.length {
                // We can use the existing space
                self.data[old_meta.offset..old_meta.offset + value.len()]
                    .copy_from_slice(value);
                self.metadata[index].length = value.len();
                return Ok(());
            }

            // Calculate space needed for the new value
            if !self.can_add(key, value.len(), true) {
                return Err("Page is full");
            }

            // Remove old metadata and compact data if necessary
            self.metadata.remove(index);
            self.compact_data();
        } else {
            // This is a new insert
            if !self.can_add(key, value.len(), false) {
                return Err("Page is full");
            }
        }

        // Calculate new metadata size
        let new_metadata_size = self.metadata_size(false);
        
        // If we need to move data to make room for new metadata
        if new_metadata_size > self.data.len() {
            // Move all data to the new position
            let data_to_move = self.data.len();
            let new_data_start = new_metadata_size;
            self.data.copy_within(0..data_to_move, new_data_start);
        }

        // Store the metadata
        let meta = KeyValueMeta {
            key,
            offset: self.data.len(),
            length: value.len(),
        };
        self.metadata.push(meta);

        // Copy the data into the page
        let start = self.data.len();
        self.data.extend_from_slice(value);

        Ok(())
    }

    // Helper method to compact data after removing entries
    fn compact_data(&mut self) {
        if self.metadata.is_empty() {
            self.data.clear();
            return;
        }

        // Sort metadata by offset
        self.metadata.sort_by_key(|m| m.offset);

        // Update data_start
        let data_start = self.metadata_size(true);

        // Compact data
        let mut new_offset = data_start;
        for meta in self.metadata.iter_mut() {
            if meta.offset != new_offset {
                // Move data to new position
                self.data.copy_within(
                    meta.offset..meta.offset + meta.length,
                    new_offset
                );
                meta.offset = new_offset;
            }
            new_offset += meta.length;
        }
        self.data.truncate(new_offset - data_start);
    }

    // Method to remove a key-value pair
    pub fn delete(&mut self, key: u64) -> Result<(), KeyNotFoundError> {
        if let Some(index) = self.metadata.iter().position(|m| m.key == key) {
            self.metadata.remove(index);
            self.compact_data();
            Ok(())
        } else {
            Err(KeyNotFoundError)
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

    pub fn split(&mut self) -> (LeafPage, Vec<u8>) {
        // Create new page with same size
        let mut new_page = LeafPage::new(self.page_size);
        
        // Sort metadata by key for consistent splitting
        self.metadata.sort_by_key(|m| m.key);
        
        // Split metadata in half
        let split_point = self.metadata.len() / 2;
        let new_metadata = self.metadata.split_off(split_point);
        
        // Calculate new data start for both pages
        let mut current_data_start = 0;
        let mut new_data_start = 0;
        
        // Update metadata and data for both pages
        for meta in &mut self.metadata {
            let data = &self.data[meta.offset..meta.offset + meta.length];
            meta.offset = current_data_start;
            current_data_start += meta.length;
        }
        
        for meta in &new_metadata {
            let data = &self.data[meta.offset..meta.offset + meta.length];
            meta.offset = new_data_start;
            new_data_start += meta.length;
        }
        
        // Set metadata and data for new page
        new_page.metadata = new_metadata;
        new_page.data = self.data.clone();
        
        // Return the new page and the key that split the pages
        let split_key = self.metadata.last().unwrap().key;
        (new_page, split_key.to_vec())
    }
} 