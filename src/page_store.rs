use std::error::Error;
use std::collections::HashMap;
use crate::leaf_page::LeafPage;
use crc::{Crc, CRC_32_ISCSI};

pub const DEFAULT_PAGE_SIZE: usize = 4096; // 4KB default page size

// CRC-32/ISCSI is a good choice for data integrity checks
const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

// Trait for storing and retrieving pages
pub trait PageStore {
    fn get_page_bytes(&self, page_id: u64) -> Option<Vec<u8>>;
    fn put_page_bytes(&mut self, page_id: u64, bytes: &[u8]) -> Option<()>;
    fn allocate_page(&mut self) -> u64;
    fn flush(&mut self) -> Result<(), Box<dyn Error>>;
    fn page_size(&self) -> usize;
    fn get_next_page_id(&self, page_id: u64) -> Option<u64>;
    fn get_prev_page_id(&self, page_id: u64) -> Option<u64>;
    fn link_pages(&mut self, prev_page_id: u64, next_page_id: u64) -> Result<(), Box<dyn Error>>;
    fn page_exists(&self, page_id: u64) -> bool;
    fn free_page(&mut self, page_id: u64) -> Result<(), Box<dyn Error>>;
}

// In-memory implementation of PageStore for testing
#[derive(Clone, Debug)]
pub struct InMemoryPageStore {
    pages: HashMap<u64, Vec<u8>>,
    next_page_id: u64,
    page_size: usize,
}

impl InMemoryPageStore {
    pub fn new() -> Self {
        Self::with_page_size(DEFAULT_PAGE_SIZE)
    }

    pub fn with_page_size(page_size: usize) -> Self {
        InMemoryPageStore {
            pages: HashMap::new(),
            next_page_id: 0,
            page_size,
        }
    }

    #[cfg(test)]
    pub(crate) fn pages(&mut self) -> &mut HashMap<u64, Vec<u8>> {
        &mut self.pages
    }

    fn calculate_crc(&self, data: &[u8]) -> u32 {
        CRC.checksum(data)
    }

    fn verify_crc(&self, data: &[u8], expected_crc: u32) -> bool {
        self.calculate_crc(data) == expected_crc
    }

    pub fn page_exists(&self, page_id: u64) -> bool {
        self.pages.contains_key(&page_id)
    }

    pub fn free_page(&mut self, page_id: u64) -> Result<(), Box<dyn Error>> {
        // Remove the page from the store
        self.pages.remove(&page_id);
        Ok(())
    }
}

impl PageStore for InMemoryPageStore {
    fn get_page_bytes(&self, page_id: u64) -> Option<Vec<u8>> {
        self.pages.get(&page_id).map(|page| {
            // Split the page into data and CRC
            let (data, crc_bytes) = page.split_at(page.len() - 4);
            let expected_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
            
            // Verify CRC
            if !self.verify_crc(data, expected_crc) {
                panic!("CRC check failed for page {}", page_id);
            }
            
            data.to_vec()
        })
    }

    fn put_page_bytes(&mut self, page_id: u64, bytes: &[u8]) -> Option<()> {
        // Ensure data fits in page size minus CRC size
        if bytes.len() > self.page_size - 4 {
            return None;
        }

        // Calculate and append CRC
        let crc = self.calculate_crc(bytes);
        let mut page = bytes.to_vec();
        page.extend_from_slice(&crc.to_le_bytes());

        // Ensure the final page size is correct
        if page.len() > self.page_size {
            return None;
        }

        self.pages.insert(page_id, page);
        Some(())
    }

    fn allocate_page(&mut self) -> u64 {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        
        // Create an empty page with proper CRC
        let empty_page = LeafPage::new(self.page_size - 4).serialize();
        let crc = self.calculate_crc(&empty_page);
        let mut page = empty_page;
        page.extend_from_slice(&crc.to_le_bytes());
        
        self.pages.insert(page_id, page);
        page_id
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn page_size(&self) -> usize {
        self.page_size
    }

    fn get_next_page_id(&self, page_id: u64) -> Option<u64> {
        self.get_page_bytes(page_id).map(|bytes| {
            let page = LeafPage::deserialize(&bytes);
            page.next_page_id()
        })
    }

    fn get_prev_page_id(&self, page_id: u64) -> Option<u64> {
        self.get_page_bytes(page_id).map(|bytes| {
            let page = LeafPage::deserialize(&bytes);
            page.prev_page_id()
        })
    }

    fn link_pages(&mut self, prev_page_id: u64, next_page_id: u64) -> Result<(), Box<dyn Error>> {
        // Update prev page's next pointer
        if let Some(prev_bytes) = self.get_page_bytes(prev_page_id) {
            let mut prev_page = LeafPage::deserialize(&prev_bytes);
            prev_page.set_next_page_id(next_page_id);
            self.put_page_bytes(prev_page_id, &prev_page.serialize());
        }

        // Update next page's prev pointer
        if let Some(next_bytes) = self.get_page_bytes(next_page_id) {
            let mut next_page = LeafPage::deserialize(&next_bytes);
            next_page.set_prev_page_id(prev_page_id);
            self.put_page_bytes(next_page_id, &next_page.serialize());
        }

        Ok(())
    }
} 