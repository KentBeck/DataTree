use std::collections::HashSet;
use std::error::Error;
use crate::leaf_page::LeafPage;
use crate::page_store::PageStore;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    LeafPage = 1,
    BranchPage = 2,
    // Future page types will be added here
}

impl PageType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(PageType::LeafPage),
            2 => Some(PageType::BranchPage),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

pub struct DataTree<S: PageStore> {
    store: S,
    root_page_id: u64,
    dirty_pages: HashSet<u64>, // Track which pages have been modified
}

impl<S: PageStore> DataTree<S> {
    pub fn new(mut store: S) -> Self {
        let root_page_id = store.allocate_page();
        let root_page = LeafPage::new(store.page_size());
        store.put_page_bytes(root_page_id, &root_page.serialize()).unwrap();
        DataTree {
            store,
            root_page_id,
            dirty_pages: HashSet::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        let mut current_page_id = self.root_page_id;
        loop {
            let page_bytes = self.store.get_page_bytes(current_page_id)?;
            let page = LeafPage::deserialize(&page_bytes);

            if let Some(value) = page.get(key) {
                return Ok(Some(value.to_vec()));
            }

            if let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                current_page_id = next_page_id;
            } else {
                return Ok(None);
            }
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
        // Check if value is too large for a page
        let page = LeafPage::new(self.store.page_size());
        if page.is_value_too_large(value) {
            return Err("Value too large for page size".into());
        }

        let mut current_page_id = self.root_page_id;
        loop {
            let page_bytes = self.store.get_page_bytes(current_page_id)?;
            let mut page = LeafPage::deserialize(&page_bytes);

            if page.insert(key, value) {
                self.store.put_page_bytes(current_page_id, &page.serialize())?;
                self.dirty_pages.insert(current_page_id);
                return Ok(());
            }

            // Try to split the page if it's full
            if let Some(mut new_page) = page.split() {
                // Create new page
                let new_page_id = self.store.allocate_page();

                // Update links
                let next_page_id = page.next_page_id();
                new_page.set_prev_page_id(current_page_id);
                new_page.set_next_page_id(next_page_id);

                // Save the new page
                self.store.put_page_bytes(new_page_id, &new_page.serialize())?;
                self.dirty_pages.insert(new_page_id);

                // Update current page's next pointer
                page.set_next_page_id(new_page_id);
                self.store.put_page_bytes(current_page_id, &page.serialize())?;
                self.dirty_pages.insert(current_page_id);

                // Update next page's prev pointer if it exists
                if next_page_id != 0 {
                    let next_bytes = self.store.get_page_bytes(next_page_id)?;
                    let mut next_page = LeafPage::deserialize(&next_bytes);
                    next_page.set_prev_page_id(new_page_id);
                    self.store.put_page_bytes(next_page_id, &next_page.serialize())?;
                    self.dirty_pages.insert(next_page_id);
                }

                // Try to insert into the current page again
                if page.insert(key, value) {
                    self.store.put_page_bytes(current_page_id, &page.serialize())?;
                    self.dirty_pages.insert(current_page_id);
                    return Ok(());
                }

                // If it doesn't fit in the current page, try the new page
                if new_page.insert(key, value) {
                    self.store.put_page_bytes(new_page_id, &new_page.serialize())?;
                    self.dirty_pages.insert(new_page_id);
                    return Ok(());
                }
            }

            if let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                current_page_id = next_page_id;
            } else {
                // Create new page
                let new_page_id = self.store.allocate_page();
                let mut new_page = LeafPage::new(self.store.page_size());
                if !new_page.insert(key, value) {
                    return Err("Value too large for page size".into());
                }
                self.store.put_page_bytes(new_page_id, &new_page.serialize())?;
                self.store.link_pages(current_page_id, new_page_id)?;
                self.dirty_pages.insert(new_page_id);
                return Ok(());
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool, Box<dyn Error>> {
        let mut current_page_id = self.root_page_id;
        loop {
            let page_bytes = self.store.get_page_bytes(current_page_id)?;
            let mut page = LeafPage::deserialize(&page_bytes);

            if page.delete(key) {
                self.store.put_page_bytes(current_page_id, &page.serialize())?;
                self.dirty_pages.insert(current_page_id);

                // Check if page is empty and not root
                if page.metadata().is_empty() && current_page_id != self.root_page_id {
                    // Get previous and next page IDs
                    let prev_page_id = page.prev_page_id();
                    let next_page_id = page.next_page_id();

                    // Update links
                    if prev_page_id != 0 {
                        let prev_bytes = self.store.get_page_bytes(prev_page_id)?;
                        let mut prev_page = LeafPage::deserialize(&prev_bytes);
                        prev_page.set_next_page_id(next_page_id);
                        self.store.put_page_bytes(prev_page_id, &prev_page.serialize())?;
                        self.dirty_pages.insert(prev_page_id);
                    }

                    if next_page_id != 0 {
                        let next_bytes = self.store.get_page_bytes(next_page_id)?;
                        let mut next_page = LeafPage::deserialize(&next_bytes);
                        next_page.set_prev_page_id(prev_page_id);
                        self.store.put_page_bytes(next_page_id, &next_page.serialize())?;
                        self.dirty_pages.insert(next_page_id);
                    }

                    // Free the empty page
                    self.store.free_page(current_page_id)?;
                    self.dirty_pages.remove(&current_page_id);
                }

                return Ok(true);
            }

            if let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                current_page_id = next_page_id;
            } else {
                return Ok(false);
            }
        }
    }

    pub fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.dirty_pages.clear();
        Ok(())
    }

    /// Returns a reference to the set of dirty page IDs
    pub fn dirty_pages(&self) -> &HashSet<u64> {
        &self.dirty_pages
    }

    pub fn root_page_id(&self) -> u64 {
        self.root_page_id
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }
}