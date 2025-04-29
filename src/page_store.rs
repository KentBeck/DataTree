use std::error::Error;
use std::collections::HashMap;
use crate::leaf_page::LeafPage;

pub const DEFAULT_PAGE_SIZE: usize = 4096; // 4KB default page size

// Trait for storing and retrieving pages
pub trait PageStore {
    fn get_page_bytes(&self, page_id: u64) -> Option<Vec<u8>>;
    fn put_page_bytes(&mut self, page_id: u64, data: &[u8]);
    fn allocate_page(&mut self) -> u64;
    fn flush(&mut self) -> Result<(), Box<dyn Error>>;
    fn page_size(&self) -> usize;
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
}

impl PageStore for InMemoryPageStore {
    fn get_page_bytes(&self, page_id: u64) -> Option<Vec<u8>> {
        self.pages.get(&page_id).cloned()
    }

    fn put_page_bytes(&mut self, page_id: u64, data: &[u8]) {
        assert_eq!(data.len(), self.page_size, "Data size must match page size");
        self.pages.insert(page_id, data.to_vec());
    }

    fn allocate_page(&mut self) -> u64 {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        let empty_page = LeafPage::new(self.page_size).serialize();
        self.pages.insert(page_id, empty_page);
        page_id
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn page_size(&self) -> usize {
        self.page_size
    }
} 