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
    pub fn new(store: S) -> Self {
        let root_page_id = store.allocate_page();
        DataTree {
            store,
            root_page_id,
            dirty_pages: HashSet::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        let mut current_page_id = self.root_page_id;
        
        loop {
            let page_bytes = self.store.get_page_bytes(current_page_id)
                .ok_or("Page not found")?;
            let page = LeafPage::deserialize(&page_bytes);
            
            if let Some(value) = page.get(key) {
                return Ok(Some(value.to_vec()));
            }
            
            // Try next page if exists
            if let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                current_page_id = next_page_id;
            } else {
                return Ok(None);
            }
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut current_page_id = self.root_page_id;
        
        loop {
            let page_bytes = self.store.get_page_bytes(current_page_id)
                .ok_or("Page not found")?;
            let mut page = LeafPage::deserialize(&page_bytes);
            
            // Check if page is full
            if page.is_full(key, value) {
                // Split the page
                let (mut new_page, split_key) = page.split();
                
                // Create new page in store
                let new_page_id = self.store.allocate_page();
                
                // Link the pages
                self.store.link_pages(current_page_id, new_page_id)?;
                
                // Save both pages
                self.store.put_page_bytes(current_page_id, &page.serialize());
                self.store.put_page_bytes(new_page_id, &new_page.serialize());
                
                // Continue with the appropriate page based on key
                if key < &split_key {
                    current_page_id = current_page_id;
                } else {
                    current_page_id = new_page_id;
                }
                
                // Try inserting again
                continue;
            }
            
            // Try to insert into current page
            if page.insert(key, value) {
                self.store.put_page_bytes(current_page_id, &page.serialize());
                return Ok(());
            }
            
            // If current page is full, try next page
            if let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                current_page_id = next_page_id;
            } else {
                // Create new page and link it
                let new_page_id = self.store.allocate_page();
                self.store.link_pages(current_page_id, new_page_id)?;
                
                let mut new_page = LeafPage::deserialize(&self.store.get_page_bytes(new_page_id)
                    .ok_or("New page not found")?);
                new_page.insert(key, value);
                self.store.put_page_bytes(new_page_id, &new_page.serialize());
                return Ok(());
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut current_page_id = self.root_page_id;
        let mut prev_page_id = None;
        
        loop {
            let page_bytes = self.store.get_page_bytes(current_page_id)
                .ok_or("Page not found")?;
            let mut page = LeafPage::deserialize(&page_bytes);
            
            if page.delete(key) {
                // If the page is now empty and it's not the root page, remove it
                if page.metadata.is_empty() && current_page_id != self.root_page_id {
                    // Link the previous and next pages together
                    if let Some(prev_id) = prev_page_id {
                        if let Some(next_id) = self.store.get_next_page_id(current_page_id) {
                            self.store.link_pages(prev_id, next_id)?;
                        } else {
                            // If no next page, just update prev page's next pointer
                            let mut prev_page = LeafPage::deserialize(&self.store.get_page_bytes(prev_id)
                                .ok_or("Previous page not found")?);
                            prev_page.set_next_page_id(0);
                            self.store.put_page_bytes(prev_id, &prev_page.serialize());
                        }
                    }
                    
                    // Free the empty page
                    self.store.free_page(current_page_id)?;
                } else {
                    // Save the page if it's not empty or is the root page
                    self.store.put_page_bytes(current_page_id, &page.serialize());
                }
                return Ok(());
            }
            
            // Try next page if exists
            if let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                prev_page_id = Some(current_page_id);
                current_page_id = next_page_id;
            } else {
                return Ok(());
            }
        }
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

    #[cfg(test)]
    pub(crate) fn store(&mut self) -> &mut S {
        &mut self.store
    }

    #[cfg(test)]
    pub(crate) fn root_page_id(&self) -> u64 {
        self.root_page_id
    }
} 