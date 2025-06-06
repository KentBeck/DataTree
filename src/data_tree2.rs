use std::error::Error;
use crate::leaf_page::LeafPage;
use crate::page_store::PageStore;

// Formatter for DataTree2
pub struct IdentityFormatter {
    pub root_page_id: u64,
}

impl IdentityFormatter {
    pub fn new(page_id: u64) -> Self {
        IdentityFormatter { root_page_id: page_id }
    }
}

// Collection of formatters
pub struct ResultFormatter {
    pub formatters: Vec<IdentityFormatter>,
}

impl ResultFormatter {
    pub fn new(formatters: Vec<IdentityFormatter>) -> Self {
        ResultFormatter { formatters }
    }
}

// DataTree2 implementation that uses formatters
pub struct DataTree2<S: PageStore> {
    store: S,
    formatter: ResultFormatter,
}

impl<S: PageStore> DataTree2<S> {
    pub fn get_page_count(&self) -> usize {
        return self.store.get_page_count();
    }
}

impl<S: PageStore> DataTree2<S> {
    pub fn dirty_pages(&self) -> &std::collections::HashSet<u64> {
        return self.store.dirty_pages();
    }
}

impl<S: PageStore> DataTree2<S> {
    pub fn new(store: S, formatter: ResultFormatter) -> Self {
        DataTree2 {
            store,
            formatter,
        }
    }

    /// Returns a reference to the store
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns a mutable reference to the store
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn put(&mut self, key: u64, value: &[u8]) -> Result<(), Box<dyn Error>> {
        // For simplicity, just use the first formatter's page_id
        let page_id = self.formatter.formatters[0].root_page_id;

        // Create a leaf page
        let mut page = LeafPage::new_empty(self.store.page_size());

        // Put the key-value pair in the page
        page.put(key, value);

        // Save the page
        self.store.put_page_bytes(page_id, &page.serialize())?;

        Ok(())
    }

    pub fn get(&self, key: u64) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        // For simplicity, just use the first formatter's page_id
        let page_id = self.formatter.formatters[0].root_page_id;

        // Get the page
        let page_bytes = self.store.get_page_bytes(page_id)?;
        let page = LeafPage::deserialize(&page_bytes);

        // Get the value
        if let Some(value) = page.get(key) {
            Ok(Some(value.to_vec()))
        } else {
            Ok(None)
        }
    }
}
