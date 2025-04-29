use std::error::Error;
use std::collections::HashSet;
use crate::leaf_page::LeafPage;
use crate::leaf_page::KeyNotFoundError;
use crate::page_store::PageStore;

pub struct DataTree<S: PageStore> {
    store: S,
    root_page_id: u64,
    dirty_pages: HashSet<u64>, // Track which pages have been modified
}

impl<S: PageStore> DataTree<S> {
    pub fn new(mut store: S) -> Self {
        let root_page_id = store.allocate_page();
        DataTree {
            store,
            root_page_id,
            dirty_pages: HashSet::new(),
        }
    }

    pub fn get(&self, key: u64) -> Result<Vec<u8>, KeyNotFoundError> {
        let page_bytes = self.store.get_page_bytes(self.root_page_id)
            .ok_or(KeyNotFoundError)?;
        let page = LeafPage::deserialize(&page_bytes);
        let data = page.get(key)?;
        Ok(data.to_vec())
    }

    pub fn put(&mut self, key: u64, value: &[u8]) -> Result<(), Box<dyn Error>> {
        let page_bytes = self.store.get_page_bytes(self.root_page_id)
            .ok_or("Root page not found")?;
        let mut page = LeafPage::deserialize(&page_bytes);
        page.insert(key, value)?;
        self.store.put_page_bytes(self.root_page_id, &page.serialize());
        self.dirty_pages.insert(self.root_page_id);
        Ok(())
    }

    pub fn delete(&mut self, key: u64) -> Result<(), Box<dyn Error>> {
        let page_bytes = self.store.get_page_bytes(self.root_page_id)
            .ok_or("Root page not found")?;
        let mut page = LeafPage::deserialize(&page_bytes);
        page.delete(key)?;
        self.store.put_page_bytes(self.root_page_id, &page.serialize());
        self.dirty_pages.insert(self.root_page_id);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        if !self.dirty_pages.is_empty() {
            self.store.flush()?;
            self.dirty_pages.clear();
        }
        Ok(())
    }

    /// Returns a reference to the set of dirty page IDs
    pub fn dirty_pages(&self) -> &HashSet<u64> {
        &self.dirty_pages
    }
} 