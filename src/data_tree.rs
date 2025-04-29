use std::error::Error;
use crate::leaf_page::LeafPage;
use crate::leaf_page::KeyNotFoundError;
use crate::page_store::PageStore;

pub struct DataTree<S: PageStore> {
    store: S,
    root_page_id: u64,
}

impl<S: PageStore> DataTree<S> {
    pub fn new(mut store: S) -> Self {
        let root_page_id = store.allocate_page();
        DataTree {
            store,
            root_page_id,
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
        if page.is_dirty() {
            self.store.put_page_bytes(self.root_page_id, &page.serialize());
        }
        Ok(())
    }

    pub fn delete(&mut self, key: u64) -> Result<(), Box<dyn Error>> {
        let page_bytes = self.store.get_page_bytes(self.root_page_id)
            .ok_or("Root page not found")?;
        let mut page = LeafPage::deserialize(&page_bytes);
        page.delete(key)?;
        if page.is_dirty() {
            self.store.put_page_bytes(self.root_page_id, &page.serialize());
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.store.flush()
    }
} 