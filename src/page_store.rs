use std::error::Error;
use std::collections::HashMap;
use crate::leaf_page::LeafPage;
use crc::{Crc, CRC_32_ISCSI};

const DEFAULT_PAGE_SIZE: usize = 4096;

// CRC-32/ISCSI is a good choice for data integrity checks
const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug)]
pub struct PageCorruptionError;

impl std::fmt::Display for PageCorruptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Page corruption detected: CRC check failed")
    }
}

impl Error for PageCorruptionError {}

// Trait for storing and retrieving pages
pub trait PageStore {
    fn get_page_bytes(&self, page_id: u64) -> Result<Vec<u8>, Box<dyn Error>>;
    fn put_page_bytes(&mut self, page_id: u64, bytes: &[u8]) -> Result<(), Box<dyn Error>>;
    fn allocate_page(&mut self) -> u64;
    fn flush(&mut self) -> Result<(), Box<dyn Error>>;
    fn page_size(&self) -> usize;
    fn get_next_page_id(&self, page_id: u64) -> Option<u64>;
    fn get_prev_page_id(&self, page_id: u64) -> Option<u64>;
    fn link_pages(&mut self, prev_page_id: u64, next_page_id: u64) -> Result<(), Box<dyn Error>>;
    fn page_exists(&self, page_id: u64) -> bool;
    fn free_page(&mut self, page_id: u64) -> Result<(), Box<dyn Error>>;
    fn get_page_count(&self) -> usize;
}

// In-memory implementation of PageStore for testing
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
            next_page_id: 1,
            page_size,
        }
    }

    pub fn corrupt_page_for_testing(&mut self, page_id: u64) {
        if let Some(page) = self.pages.get_mut(&page_id) {
            // Flip some bits in the page to simulate corruption
            for byte in page.iter_mut() {
                *byte ^= 0xFF; // Flip all bits
            }
        }
    }

    fn calculate_crc(data: &[u8]) -> u32 {
        CRC.checksum(data)
    }

    fn verify_crc(data: &[u8], expected_crc: u32) -> bool {
        Self::calculate_crc(data) == expected_crc
    }

    fn add_crc(mut bytes: Vec<u8>) -> Vec<u8> {
        let crc = Self::calculate_crc(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn extract_and_verify_crc(bytes: &[u8]) -> Result<&[u8], Box<dyn Error>> {
        if bytes.len() < 4 {
            return Err(Box::new(PageCorruptionError));
        }
        let (data, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let expected_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        
        if !Self::verify_crc(data, expected_crc) {
            return Err(Box::new(PageCorruptionError));
        }
        
        Ok(data)
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
    fn get_page_bytes(&self, page_id: u64) -> Result<Vec<u8>, Box<dyn Error>> {
        let bytes = self.pages.get(&page_id)
            .cloned()
            .ok_or_else(|| Box::<dyn Error>::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Page not found"
            )))?;
        
        // Extract data and verify CRC
        let data = Self::extract_and_verify_crc(&bytes)?;
        Ok(data.to_vec())
    }

    fn put_page_bytes(&mut self, page_id: u64, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        if bytes.len() + 4 > self.page_size {  // +4 for CRC
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Page too large",
            )));
        }
        
        // Add CRC to the page
        let bytes_with_crc = Self::add_crc(bytes.to_vec());
        self.pages.insert(page_id, bytes_with_crc);
        Ok(())
    }

    fn allocate_page(&mut self) -> u64 {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        
        // Initialize the page with an empty LeafPage
        let page = LeafPage::new(self.page_size);
        self.put_page_bytes(page_id, &page.serialize()).unwrap();
        
        page_id
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn page_size(&self) -> usize {
        self.page_size
    }

    fn get_next_page_id(&self, page_id: u64) -> Option<u64> {
        let bytes = self.pages.get(&page_id)?;
        let page = LeafPage::deserialize(bytes);
        let next_id = page.next_page_id();
        if next_id == 0 {
            None
        } else {
            Some(next_id)
        }
    }

    fn get_prev_page_id(&self, page_id: u64) -> Option<u64> {
        let bytes = self.pages.get(&page_id)?;
        let page = LeafPage::deserialize(bytes);
        let prev_id = page.prev_page_id();
        if prev_id == 0 {
            None
        } else {
            Some(prev_id)
        }
    }

    fn link_pages(&mut self, prev_page_id: u64, next_page_id: u64) -> Result<(), Box<dyn Error>> {
        // Get and update previous page
        let prev_bytes = self.get_page_bytes(prev_page_id)?;
        let mut prev_page = LeafPage::deserialize(&prev_bytes);
        prev_page.set_next_page_id(next_page_id);
        self.put_page_bytes(prev_page_id, &prev_page.serialize())?;

        // Get and update next page
        let next_bytes = self.get_page_bytes(next_page_id)?;
        let mut next_page = LeafPage::deserialize(&next_bytes);
        next_page.set_prev_page_id(prev_page_id);
        self.put_page_bytes(next_page_id, &next_page.serialize())?;

        Ok(())
    }

    fn page_exists(&self, page_id: u64) -> bool {
        self.pages.contains_key(&page_id)
    }

    fn free_page(&mut self, page_id: u64) -> Result<(), Box<dyn Error>> {
        self.pages.remove(&page_id);
        Ok(())
    }

    fn get_page_count(&self) -> usize {
        self.pages.len()
    }
} 