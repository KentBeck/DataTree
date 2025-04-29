pub mod leaf_page;
pub mod page_store;
pub mod data_tree;

pub use leaf_page::KeyNotFoundError;
pub use page_store::{PageStore, InMemoryPageStore};
pub use data_tree::DataTree;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::InMemoryPageStore;

    // ... existing tests ...

    #[test]
    fn test_page_splitting() {
        // Create store with 100 byte pages
        let store = InMemoryPageStore::with_page_size(100);
        let mut tree = DataTree::new(store);

        // Insert data that will require multiple pages
        let keys = vec![
            b"key1".to_vec(),
            b"key2".to_vec(),
            b"key3".to_vec(),
            b"key4".to_vec(),
        ];
        let values = vec![
            b"value1".to_vec(),
            b"value2".to_vec(),
            b"value3".to_vec(),
            b"value4".to_vec(),
        ];

        // Insert all key-value pairs
        for (key, value) in keys.iter().zip(values.iter()) {
            tree.put(key, value).unwrap();
        }

        // Verify all data can be retrieved
        for (key, expected_value) in keys.iter().zip(values.iter()) {
            let retrieved_value = tree.get(key).unwrap().unwrap();
            assert_eq!(retrieved_value, *expected_value);
        }

        // Verify page linking
        let store = tree.store();
        let mut current_page_id = tree.root_page_id;
        let mut page_count = 0;

        while let Some(next_page_id) = store.get_next_page_id(current_page_id) {
            page_count += 1;
            current_page_id = next_page_id;
        }
        page_count += 1; // Count the last page

        // We should have at least 2 pages due to splitting
        assert!(page_count >= 2);
    }

    #[test]
    fn test_large_value_splitting() {
        // Create store with 100 byte pages
        let store = InMemoryPageStore::with_page_size(100);
        let mut tree = DataTree::new(store);

        // Insert a value that's too large for one page
        let large_value = vec![b'x'; 150]; // 150 bytes
        tree.put(b"large_key", &large_value).unwrap();

        // Verify the value can be retrieved
        let retrieved_value = tree.get(b"large_key").unwrap().unwrap();
        assert_eq!(retrieved_value, large_value);

        // Verify multiple pages were used
        let store = tree.store();
        let mut current_page_id = tree.root_page_id;
        let mut page_count = 0;

        while let Some(next_page_id) = store.get_next_page_id(current_page_id) {
            page_count += 1;
            current_page_id = next_page_id;
        }
        page_count += 1; // Count the last page

        // We should have at least 2 pages
        assert!(page_count >= 2);
    }

    #[test]
    fn test_page_cleanup_after_deletion() {
        // Create store with 100 byte pages
        let store = InMemoryPageStore::with_page_size(100);
        let mut tree = DataTree::new(store);

        // Insert data that will require multiple pages
        let keys = vec![
            b"key1".to_vec(),
            b"key2".to_vec(),
            b"key3".to_vec(),
            b"key4".to_vec(),
        ];
        let values = vec![
            b"value1".to_vec(),
            b"value2".to_vec(),
            b"value3".to_vec(),
            b"value4".to_vec(),
        ];

        // Insert all key-value pairs
        for (key, value) in keys.iter().zip(values.iter()) {
            tree.put(key, value).unwrap();
        }

        // Get the page IDs before deletion
        let store = tree.store();
        let mut page_ids = Vec::new();
        let mut current_page_id = tree.root_page_id;
        
        while let Some(next_page_id) = store.get_next_page_id(current_page_id) {
            page_ids.push(current_page_id);
            current_page_id = next_page_id;
        }
        page_ids.push(current_page_id);

        // We should have at least 2 pages
        assert!(page_ids.len() >= 2);
        
        // Delete all entries from the last page
        let last_page_id = *page_ids.last().unwrap();
        let last_page_bytes = store.get_page_bytes(last_page_id).unwrap();
        let last_page = LeafPage::deserialize(&last_page_bytes);
        
        // Delete all entries from the last page
        for meta in &last_page.metadata {
            let key = &last_page.data[meta.offset..meta.offset + meta.length];
            tree.delete(key).unwrap();
        }

        // Verify the page no longer exists
        assert!(!store.page_exists(last_page_id));
        
        // Verify the page is no longer in the linked list
        let mut current_page_id = tree.root_page_id;
        while let Some(next_page_id) = store.get_next_page_id(current_page_id) {
            assert_ne!(next_page_id, last_page_id);
            current_page_id = next_page_id;
        }
        
        // Verify the previous page's next pointer is updated
        let prev_page_id = page_ids[page_ids.len() - 2];
        let prev_page_bytes = store.get_page_bytes(prev_page_id).unwrap();
        let prev_page = LeafPage::deserialize(&prev_page_bytes);
        assert_eq!(prev_page.next_page_id(), 0);
    }
} 