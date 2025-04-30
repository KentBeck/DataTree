use crate::leaf_page::PageType;

#[derive(Debug, Clone, Copy)]
pub struct BranchEntry {
    pub page_id: u64,
    pub first_key: u64,
}

impl BranchEntry {
    pub fn serialize(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.page_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.first_key.to_le_bytes());
        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        let page_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let first_key = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        BranchEntry { page_id, first_key }
    }
}

#[derive(Debug)]
pub struct BranchPage {
    pub page_type: PageType,
    pub page_size: usize,
    pub entries: Vec<BranchEntry>,
    pub prev_page_id: u64,
    pub next_page_id: u64,
}

impl BranchPage {
    pub fn new(page_size: usize) -> Self {
        BranchPage {
            page_type: PageType::BranchPage,
            page_size,
            entries: Vec::new(),
            prev_page_id: 0,
            next_page_id: 0,
        }
    }

    pub fn insert(&mut self, page_id: u64, first_key: u64) -> bool {
        let entry = BranchEntry { page_id, first_key };

        // Find insertion point to maintain sorted order
        let pos = self.entries.binary_search_by_key(&first_key, |e| e.first_key)
            .unwrap_or_else(|pos| pos);

        self.entries.insert(pos, entry);
        true
    }

    pub fn find_page_id(&self, key: u64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        // If key is less than first entry's key, return first page
        if key < self.entries[0].first_key {
            return Some(self.entries[0].page_id);
        }

        // Find the entry whose range contains this key
        for i in 0..self.entries.len() {
            let current_key = self.entries[i].first_key;
            let next_key = if i + 1 < self.entries.len() {
                self.entries[i + 1].first_key
            } else {
                u64::MAX
            };

            if key >= current_key && key < next_key {
                return Some(self.entries[i].page_id);
            }
        }

        // If we get here, the key is in the last page
        Some(self.entries.last().unwrap().page_id)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.page_size);

        // Write page type (1 byte)
        bytes.push(self.page_type.to_u8());

        // Write number of entries (8 bytes)
        bytes.extend_from_slice(&(self.entries.len() as u64).to_le_bytes());

        // Write prev_page_id (8 bytes)
        bytes.extend_from_slice(&self.prev_page_id.to_le_bytes());

        // Write next_page_id (8 bytes)
        bytes.extend_from_slice(&self.next_page_id.to_le_bytes());

        // Write entries
        for entry in &self.entries {
            bytes.extend_from_slice(&entry.serialize());
        }

        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        let mut offset = 0;

        // Read page type (1 byte)
        let page_type = PageType::from_u8(bytes[offset]).unwrap_or(PageType::BranchPage);
        offset += 1;

        // Read number of entries (8 bytes)
        let count = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read prev_page_id (8 bytes)
        let prev_page_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read next_page_id (8 bytes)
        let next_page_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read entries
        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let entry_bytes = &bytes[offset..offset + 16];
            entries.push(BranchEntry::deserialize(entry_bytes));
            offset += 16;
        }

        BranchPage {
            page_type,
            page_size: bytes.len(),
            entries,
            prev_page_id,
            next_page_id,
        }
    }

    pub fn page_type(&self) -> PageType {
        self.page_type
    }

    pub fn entries(&self) -> &[BranchEntry] {
        &self.entries
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_branch_page_operations() {
        // Create a branch page
        let mut branch_page = BranchPage::new(100);

        // Insert some entries
        assert!(branch_page.insert(1, 10)); // Page 1 starts with key 10
        assert!(branch_page.insert(2, 20)); // Page 2 starts with key 20
        assert!(branch_page.insert(3, 30)); // Page 3 starts with key 30

        // Test finding page IDs
        assert_eq!(branch_page.find_page_id(5), Some(1)); // Before first key
        assert_eq!(branch_page.find_page_id(10), Some(1)); // First key
        assert_eq!(branch_page.find_page_id(15), Some(1)); // Between 10 and 20
        assert_eq!(branch_page.find_page_id(20), Some(2)); // Second key
        assert_eq!(branch_page.find_page_id(25), Some(2)); // Between 20 and 30
        assert_eq!(branch_page.find_page_id(30), Some(3)); // Last key
        assert_eq!(branch_page.find_page_id(35), Some(3)); // After last key

        // Test serialization and deserialization
        let serialized = branch_page.serialize();
        let deserialized = BranchPage::deserialize(&serialized);

        // Verify page type
        assert_eq!(deserialized.page_type(), PageType::BranchPage);

        // Verify entries through find_page_id
        assert_eq!(deserialized.find_page_id(10), Some(1));
        assert_eq!(deserialized.find_page_id(20), Some(2));
        assert_eq!(deserialized.find_page_id(30), Some(3));
    }

    #[test]
    fn test_branch_page_linking() {
        let mut branch_page = BranchPage::new(100);

        // Test page linking
        branch_page.set_prev_page_id(42);
        branch_page.set_next_page_id(43);

        assert_eq!(branch_page.prev_page_id(), 42);
        assert_eq!(branch_page.next_page_id(), 43);

        // Verify links are preserved in serialization
        let serialized = branch_page.serialize();
        let deserialized = BranchPage::deserialize(&serialized);

        assert_eq!(deserialized.prev_page_id(), 42);
        assert_eq!(deserialized.next_page_id(), 43);
    }
}