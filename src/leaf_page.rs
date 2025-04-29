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

pub struct LeafPage {
    data: Vec<u8>,
    metadata: Vec<KeyValueMeta>,
    data_start: usize, // Start of data section after metadata
    used_bytes: usize, // Used bytes in data section
    page_size: usize, // Size of the page in bytes
}

impl LeafPage {
    pub fn new(page_size: usize) -> Self {
        LeafPage {
            data: vec![0; page_size],
            metadata: Vec::new(),
            data_start: 0,
            used_bytes: 0,
            page_size,
        }
    }

    // Serialize the page into raw bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![0; self.page_size];
        
        // First 8 bytes: number of metadata entries (u64)
        let meta_count = self.metadata.len() as u64;
        bytes[0..8].copy_from_slice(&meta_count.to_le_bytes());
        
        // Calculate header size (metadata entries start after this)
        let header_size = 24 + (meta_count as usize * 24); // 24 = 8 (count) + 8 (data_start) + 8 (used_bytes)
        
        // Next 8 bytes: data_start (u64)
        bytes[8..16].copy_from_slice(&(header_size as u64).to_le_bytes());
        
        // Next 8 bytes: used_bytes (u64)
        bytes[16..24].copy_from_slice(&(self.used_bytes as u64).to_le_bytes());
        
        println!("Serializing: meta_count={}, header_size={}, used_bytes={}", 
                meta_count, header_size, self.used_bytes);
        
        // Write metadata entries
        let mut offset = 24;
        let mut data_offset = header_size;
        
        // First copy all the data
        for meta in &self.metadata {
            let data = &self.data[meta.offset..meta.offset + meta.length];
            bytes[data_offset..data_offset + meta.length].copy_from_slice(data);
            
            // Write metadata entry
            println!("Serializing metadata: key={}, new_offset={}, length={}", 
                    meta.key, data_offset, meta.length);
            
            // key (8 bytes)
            bytes[offset..offset+8].copy_from_slice(&meta.key.to_le_bytes());
            offset += 8;
            // offset (8 bytes)
            bytes[offset..offset+8].copy_from_slice(&(data_offset as u64).to_le_bytes());
            offset += 8;
            // length (8 bytes)
            bytes[offset..offset+8].copy_from_slice(&(meta.length as u64).to_le_bytes());
            offset += 8;
            
            data_offset += meta.length;
        }
        
        bytes
    }

    // Deserialize raw bytes into a page
    pub fn deserialize(bytes: &[u8]) -> Self {
        let page_size = bytes.len();
        let mut page = LeafPage::new(page_size);
        
        // Read number of metadata entries
        let meta_count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        
        // Read data_start
        page.data_start = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        
        // Read used_bytes
        page.used_bytes = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        
        println!("Deserializing: meta_count={}, data_start={}, used_bytes={}", 
                meta_count, page.data_start, page.used_bytes);
        
        // Read metadata entries
        let mut offset = 24;
        for i in 0..meta_count {
            let key_bytes: [u8; 8] = bytes[offset..offset+8].try_into().unwrap();
            let key = u64::from_le_bytes(key_bytes);
            offset += 8;
            
            let offset_bytes: [u8; 8] = bytes[offset..offset+8].try_into().unwrap();
            let meta_offset = u64::from_le_bytes(offset_bytes) as usize;
            offset += 8;
            
            let length_bytes: [u8; 8] = bytes[offset..offset+8].try_into().unwrap();
            let length = u64::from_le_bytes(length_bytes) as usize;
            offset += 8;
            
            println!("Deserializing metadata {}: key={}, offset={}, length={}", 
                    i, key, meta_offset, length);
            
            // Copy the data to its final location in the page
            page.data[meta_offset..meta_offset + length]
                .copy_from_slice(&bytes[meta_offset..meta_offset + length]);
            
            page.metadata.push(KeyValueMeta {
                key,
                offset: meta_offset,
                length,
            });
        }
        
        page
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
                    self.used_bytes // No additional space needed
                } else {
                    self.used_bytes + (data_size - existing_meta.length)
                }
            } else {
                self.used_bytes + data_size
            }
        } else {
            self.used_bytes + data_size
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
        if new_metadata_size > self.data_start {
            // Move all data to the new position
            let data_to_move = self.used_bytes;
            let new_data_start = new_metadata_size;
            self.data.copy_within(self.data_start..self.data_start + data_to_move, new_data_start);
            
            // Update metadata offsets
            for meta in &mut self.metadata {
                meta.offset = new_data_start + (meta.offset - self.data_start);
            }
            
            self.data_start = new_data_start;
        }

        // Store the metadata
        let meta = KeyValueMeta {
            key,
            offset: self.data_start + self.used_bytes,
            length: value.len(),
        };
        self.metadata.push(meta);

        // Copy the data into the page
        let start = self.data_start + self.used_bytes;
        self.data[start..start + value.len()].copy_from_slice(value);
        self.used_bytes += value.len();

        Ok(())
    }

    // Helper method to compact data after removing entries
    fn compact_data(&mut self) {
        if self.metadata.is_empty() {
            self.used_bytes = 0;
            self.data_start = 0;
            return;
        }

        // Sort metadata by offset
        self.metadata.sort_by_key(|m| m.offset);

        // Update data_start
        self.data_start = self.metadata_size(true);

        // Compact data
        let mut new_offset = self.data_start;
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
        self.used_bytes = new_offset - self.data_start;
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
} 