use std::collections::HashSet;
use std::error::Error;
use crate::leaf_page::LeafPage;
use crate::branch_page::BranchPage;
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
        // For now, just create a LeafPage as the root for compatibility with tests
        let root_page_id = store.allocate_page();
        let root_page = LeafPage::new(store.page_size());
        store.put_page_bytes(root_page_id, &root_page.serialize()).unwrap();

        DataTree {
            store,
            root_page_id,
            dirty_pages: HashSet::new(),
        }
    }

    // This method creates a DataTree with a BranchPage as the root
    pub fn new_with_branch_root(mut store: S) -> Self {
        // Allocate a page for the leaf page
        let leaf_page_id = store.allocate_page();
        let leaf_page = LeafPage::new(store.page_size());
        store.put_page_bytes(leaf_page_id, &leaf_page.serialize()).unwrap();

        // Allocate a page for the branch page (root)
        let root_page_id = store.allocate_page();
        let mut branch_page = BranchPage::new(store.page_size());

        // Add the leaf page as the first entry in the branch page
        // Use 0 as the first_key since it's an empty leaf page
        branch_page.insert(leaf_page_id, 0);

        // Save the branch page
        store.put_page_bytes(root_page_id, &branch_page.serialize()).unwrap();

        DataTree {
            store,
            root_page_id,
            dirty_pages: HashSet::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        // Start with the root page (which is a BranchPage)
        let root_page_bytes = self.store.get_page_bytes(self.root_page_id)?;

        // Check the page type
        let page_type = PageType::from_u8(root_page_bytes[0]).unwrap_or(PageType::LeafPage);

        if page_type == PageType::BranchPage {
            // It's a branch page, find the appropriate leaf page
            let branch_page = BranchPage::deserialize(&root_page_bytes);

            // Convert key to u64 for branch page lookup
            // For simplicity, we'll use the first 8 bytes of the key as a u64
            // In a real implementation, you might want a better hashing strategy
            let key_as_u64 = if key.len() >= 8 {
                u64::from_le_bytes(key[0..8].try_into().unwrap())
            } else {
                // Pad with zeros if key is shorter than 8 bytes
                let mut padded = [0u8; 8];
                for (i, &b) in key.iter().enumerate() {
                    padded[i] = b;
                }
                u64::from_le_bytes(padded)
            };

            // Find the leaf page ID using the branch page
            if let Some(leaf_page_id) = branch_page.find_page_id(key_as_u64) {
                // Now get the leaf page
                let leaf_page_bytes = self.store.get_page_bytes(leaf_page_id)?;
                let leaf_page = LeafPage::deserialize(&leaf_page_bytes);

                // Look for the key in the leaf page
                if let Some(value) = leaf_page.get(key) {
                    return Ok(Some(value.to_vec()));
                }

                // Check if there are more leaf pages to search
                let mut current_page_id = leaf_page_id;
                while let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                    current_page_id = next_page_id;
                    let page_bytes = self.store.get_page_bytes(current_page_id)?;
                    let page = LeafPage::deserialize(&page_bytes);

                    if let Some(value) = page.get(key) {
                        return Ok(Some(value.to_vec()));
                    }
                }
            }

            // Key not found
            return Ok(None);
        } else {
            // For backward compatibility, handle the case where root is a LeafPage
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
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
        // Check if value is too large for a page
        let page = LeafPage::new(self.store.page_size());
        if page.is_value_too_large(value) {
            return Err("Value too large for page size".into());
        }

        // Start with the root page (which is a BranchPage)
        let root_page_bytes = self.store.get_page_bytes(self.root_page_id)?;

        // Check the page type
        let page_type = PageType::from_u8(root_page_bytes[0]).unwrap_or(PageType::LeafPage);

        if page_type == PageType::BranchPage {
            // It's a branch page, find the appropriate leaf page
            let branch_page = BranchPage::deserialize(&root_page_bytes);

            // Convert key to u64 for branch page lookup
            let key_as_u64 = if key.len() >= 8 {
                u64::from_le_bytes(key[0..8].try_into().unwrap())
            } else {
                // Pad with zeros if key is shorter than 8 bytes
                let mut padded = [0u8; 8];
                for (i, &b) in key.iter().enumerate() {
                    padded[i] = b;
                }
                u64::from_le_bytes(padded)
            };

            // Find the leaf page ID using the branch page
            if let Some(leaf_page_id) = branch_page.find_page_id(key_as_u64) {
                // Now try to insert into the leaf page
                let mut current_page_id = leaf_page_id;
                loop {
                    let page_bytes = self.store.get_page_bytes(current_page_id)?;
                    let mut page = LeafPage::deserialize(&page_bytes);

                    // Try to insert into this page
                    if page.insert(key, value) {
                        // Successfully inserted, update the page
                        self.store.put_page_bytes(current_page_id, &page.serialize())?;
                        self.dirty_pages.insert(current_page_id);
                        return Ok(());
                    }

                    // Page is full, check if there's a next page
                    if let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                        current_page_id = next_page_id;
                    } else {
                        // No next page, create a new one
                        let new_page_id = self.store.allocate_page();
                        let mut new_page = LeafPage::new(self.store.page_size());

                        // Update the current page to point to the new page
                        page.set_next_page_id(new_page_id);
                        self.store.put_page_bytes(current_page_id, &page.serialize())?;
                        self.dirty_pages.insert(current_page_id);

                        // Update the new page to point back to the current page
                        new_page.set_prev_page_id(current_page_id);

                        // Insert the key-value pair into the new page
                        if !new_page.insert(key, value) {
                            return Err("Failed to insert into new page".into());
                        }

                        // Save the new page
                        self.store.put_page_bytes(new_page_id, &new_page.serialize())?;
                        self.dirty_pages.insert(new_page_id);
                        return Ok(());
                    }
                }
            } else {
                // This should not happen with our implementation, but handle it anyway
                return Err("Could not find a leaf page for the key".into());
            }
        } else {
            // For backward compatibility, handle the case where root is a LeafPage
            let mut current_page_id = self.root_page_id;
            loop {
                let page_bytes = self.store.get_page_bytes(current_page_id)?;
                let mut page = LeafPage::deserialize(&page_bytes);

                // Try to insert into this page
                if page.insert(key, value) {
                    // Successfully inserted, update the page
                    self.store.put_page_bytes(current_page_id, &page.serialize())?;
                    self.dirty_pages.insert(current_page_id);
                    return Ok(());
                }
                // Page is full, check if there's a next page
                if let Some(next_page_id) = self.store.get_next_page_id(current_page_id) {
                    current_page_id = next_page_id;
                } else {
                    // No next page, create a new one
                    let new_page_id = self.store.allocate_page();
                    let mut new_page = LeafPage::new(self.store.page_size());

                    // Update the current page to point to the new page
                    page.set_next_page_id(new_page_id);
                    self.store.put_page_bytes(current_page_id, &page.serialize())?;
                    self.dirty_pages.insert(current_page_id);

                    // Update the new page to point back to the current page
                    new_page.set_prev_page_id(current_page_id);

                    // Insert the key-value pair into the new page
                    if !new_page.insert(key, value) {
                        return Err("Failed to insert into new page".into());
                    }

                    // Save the new page
                    self.store.put_page_bytes(new_page_id, &new_page.serialize())?;
                    self.dirty_pages.insert(new_page_id);
                    return Ok(());
                }
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool, Box<dyn Error>> {
        // Start with the root page (which is a BranchPage)
        let root_page_bytes = self.store.get_page_bytes(self.root_page_id)?;

        // Check the page type
        let page_type = PageType::from_u8(root_page_bytes[0]).unwrap_or(PageType::LeafPage);

        if page_type == PageType::BranchPage {
            // It's a branch page, find the appropriate leaf page
            let branch_page = BranchPage::deserialize(&root_page_bytes);

            // Convert key to u64 for branch page lookup
            let key_as_u64 = if key.len() >= 8 {
                u64::from_le_bytes(key[0..8].try_into().unwrap())
            } else {
                // Pad with zeros if key is shorter than 8 bytes
                let mut padded = [0u8; 8];
                for (i, &b) in key.iter().enumerate() {
                    padded[i] = b;
                }
                u64::from_le_bytes(padded)
            };

            // Find the leaf page ID using the branch page
            if let Some(leaf_page_id) = branch_page.find_page_id(key_as_u64) {
                // Now try to delete from the leaf page
                let mut current_page_id = leaf_page_id;
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
            } else {
                // This should not happen with our implementation, but handle it anyway
                return Ok(false);
            }
        } else {
            // For backward compatibility, handle the case where root is a LeafPage
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
    }

    pub fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.dirty_pages.clear();
        Ok(())
    }

    /// Returns a reference to the set of dirty page IDs
    pub fn dirty_pages(&self) -> &HashSet<u64> {
        &self.dirty_pages
    }

    /// Returns a reference to the store
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns the root page ID
    pub fn root_page_id(&self) -> u64 {
        self.root_page_id
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }
}