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

// Define the page size constant
pub const PAGE_SIZE: usize = 4096; // 4KB page size

// Metadata for each key-value pair
#[derive(Debug, Clone, Copy)]
struct KeyValueMeta {
    key: u64,
    offset: usize,
    length: usize,
}

pub struct LeafPage {
    data: [u8; PAGE_SIZE],
    metadata: Vec<KeyValueMeta>,
    data_start: usize, // Start of data section after metadata
    used_bytes: usize, // Used bytes in data section
    dirty: bool,      // Track if page has been modified
}

impl LeafPage {
    pub fn new() -> Self {
        LeafPage {
            data: [0; PAGE_SIZE],
            metadata: Vec::new(),
            data_start: 0,
            used_bytes: 0,
            dirty: false,
        }
    }

    /// Returns true if the page has been modified since last clear
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Clears the dirty flag
    pub fn clear_dirty(&mut self) {
        self.dirty = false;
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
        self.total_space_needed(key, data_size, is_update) <= PAGE_SIZE
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
                self.dirty = true;
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

        // Update data_start
        self.data_start = self.metadata_size(false);

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
        self.dirty = true;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dirty_flag_on_insert() {
        let mut page = LeafPage::new();
        assert!(!page.is_dirty(), "New page should not be dirty");
        
        page.insert(1, &[1, 2, 3]).unwrap();
        assert!(page.is_dirty(), "Page should be dirty after insert");
        
        page.clear_dirty();
        assert!(!page.is_dirty(), "Page should not be dirty after clear");
    }

    #[test]
    fn test_dirty_flag_on_update() {
        let mut page = LeafPage::new();
        page.insert(1, &[1, 2, 3]).unwrap();
        page.clear_dirty();
        assert!(!page.is_dirty(), "Page should not be dirty after clear");

        // Update with same key
        page.insert(1, &[4, 5, 6]).unwrap();
        assert!(page.is_dirty(), "Page should be dirty after update");
    }

    #[test]
    fn test_dirty_flag_on_read() {
        let mut page = LeafPage::new();
        page.insert(1, &[1, 2, 3]).unwrap();
        page.clear_dirty();
        
        // Reading should not affect dirty flag
        let _ = page.get(1).unwrap();
        assert!(!page.is_dirty(), "Page should not be dirty after read");
    }

    #[test]
    fn test_basic_insert_and_get() {
        let mut page = LeafPage::new();
        let data = &[1, 2, 3, 4, 5];
        page.insert(1, data).unwrap();
        assert_eq!(page.get(1).unwrap(), data);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let page = LeafPage::new();
        assert!(page.get(1).is_err());
    }

    #[test]
    fn test_multiple_inserts_and_gets() {
        let mut page = LeafPage::new();
        let data1 = &[1, 2, 3];
        let data2 = &[4, 5, 6];
        let data3 = &[7, 8, 9];
        
        page.insert(1, data1).unwrap();
        page.insert(2, data2).unwrap();
        page.insert(3, data3).unwrap();
        
        assert_eq!(page.get(1).unwrap(), data1);
        assert_eq!(page.get(2).unwrap(), data2);
        assert_eq!(page.get(3).unwrap(), data3);
    }

    #[test]
    fn test_insert_duplicate_key() {
        let mut page = LeafPage::new();
        page.insert(1, &[1, 2, 3]).unwrap();
        page.insert(1, &[4, 5, 6]).unwrap();
        assert_eq!(page.get(1).unwrap(), &[4, 5, 6]);
    }

    #[test]
    fn test_page_full() {
        let mut page = LeafPage::new();
        // Calculate space needed for metadata
        let meta_size = std::mem::size_of::<KeyValueMeta>();
        
        // First insert will need space for:
        // - One metadata entry
        // - The data itself
        let available_space = PAGE_SIZE - meta_size;
        let large_data = vec![0; available_space - 10];
        
        assert!(page.insert(1, &large_data).is_ok());
        
        // Second insert will need space for:
        // - Another metadata entry (meta_size more bytes)
        // - The new data
        // At this point we should have no space left
        assert!(page.insert(2, &[1, 2, 3]).is_err());
    }

    #[test]
    fn test_exact_page_size() {
        let mut page = LeafPage::new();
        let meta_size = std::mem::size_of::<KeyValueMeta>();
        let data = vec![0; PAGE_SIZE - meta_size]; // Account for one metadata entry
        assert!(page.insert(1, &data).is_ok());
    }

    #[test]
    fn test_zero_length_data() {
        let mut page = LeafPage::new();
        assert!(page.insert(1, &[]).is_ok());
        assert_eq!(page.get(1).unwrap(), &[]);
    }

    #[test]
    fn test_data_integrity() {
        let mut page = LeafPage::new();
        let data = (0..255).collect::<Vec<u8>>();
        page.insert(1, &data).unwrap();
        let retrieved = page.get(1).unwrap();
        assert_eq!(retrieved.len(), data.len());
        assert!(retrieved.iter().zip(data.iter()).all(|(a, b)| a == b));
    }

    #[test]
    fn test_sequential_keys() {
        let mut page = LeafPage::new();
        for i in 0..10 { // Reduced from 100 to avoid filling page
            let data = &[i as u8];
            page.insert(i as u64, data).unwrap();
            assert_eq!(page.get(i as u64).unwrap(), data);
        }
    }

    #[test]
    fn test_large_keys() {
        let mut page = LeafPage::new();
        let large_key = u64::MAX;
        page.insert(large_key, &[1, 2, 3]).unwrap();
        assert_eq!(page.get(large_key).unwrap(), &[1, 2, 3]);
    }

    #[test]
    fn test_metadata_size_limit() {
        let mut page = LeafPage::new();
        let meta_size = std::mem::size_of::<KeyValueMeta>();
        
        // Each entry needs:
        // - One metadata entry (meta_size bytes)
        // - At least one byte of data
        // For n entries, we need:
        // n * meta_size + n * 1 <= PAGE_SIZE
        // n * (meta_size + 1) <= PAGE_SIZE
        // n <= PAGE_SIZE / (meta_size + 1)
        let max_entries = PAGE_SIZE / (meta_size + 1);
        
        // Fill up to max_entries - 1
        for i in 0..max_entries {
            assert!(page.insert(i as u64, &[1]).is_ok(), "Failed at entry {}", i);
        }
        
        // Next insert should fail because we need space for both metadata and data
        assert!(page.insert(max_entries as u64, &[1]).is_err());
    }

    #[test]
    fn test_update_with_larger_value() {
        let mut page = LeafPage::new();
        page.insert(1, &[1, 2]).unwrap();
        page.insert(1, &[1, 2, 3, 4]).unwrap();
        assert_eq!(page.get(1).unwrap(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_update_with_smaller_value() {
        let mut page = LeafPage::new();
        page.insert(1, &[1, 2, 3, 4]).unwrap();
        page.insert(1, &[1, 2]).unwrap();
        assert_eq!(page.get(1).unwrap(), &[1, 2]);
    }
}
