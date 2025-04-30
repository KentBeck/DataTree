pub mod leaf_page;
pub mod page_store;
pub mod data_tree;
pub mod branch_page;

pub use leaf_page::{KeyNotFoundError, LeafPage, PageType};
pub use page_store::{PageStore, InMemoryPageStore};
pub use data_tree::DataTree;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::InMemoryPageStore;

    #[test]
    fn test_basic_operations() {
        let store = InMemoryPageStore::new();
        let mut tree = DataTree::new(store);

        // Test put and get
        tree.put(b"key1", b"value1").unwrap();
        assert_eq!(tree.get(b"key1").unwrap().unwrap(), b"value1");

        // Test update
        tree.put(b"key1", b"new_value1").unwrap();
        assert_eq!(tree.get(b"key1").unwrap().unwrap(), b"new_value1");

        // Test delete
        tree.delete(b"key1").unwrap();
        assert!(tree.get(b"key1").unwrap().is_none());
    }

    #[test]
    fn test_error_handling() {
        let store = InMemoryPageStore::new();
        let mut tree = DataTree::new(store);

        // Test non-existent key
        assert!(tree.get(b"nonexistent").unwrap().is_none());

        // Test delete non-existent key - should succeed (idempotent operation)
        assert!(tree.delete(b"nonexistent").is_ok());
    }
} 