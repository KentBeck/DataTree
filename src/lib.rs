pub mod leaf_page;
pub mod page_store;
pub mod data_tree;

#[cfg(test)]
mod tests {
    use super::*;
    use page_store::InMemoryPageStore;
    use data_tree::DataTree;

    #[test]
    fn test_basic_operations() {
        let store = InMemoryPageStore::new();
        let mut tree = DataTree::new(store);

        // Test insert and get
        tree.put(1, b"hello").unwrap();
        assert_eq!(tree.get(1).unwrap(), b"hello");

        // Test update
        tree.put(1, b"world").unwrap();
        assert_eq!(tree.get(1).unwrap(), b"world");

        // Test delete
        tree.delete(1).unwrap();
        assert!(tree.get(1).is_err());

        // Test multiple keys
        tree.put(1, b"one").unwrap();
        tree.put(2, b"two").unwrap();
        tree.put(3, b"three").unwrap();
        assert_eq!(tree.get(1).unwrap(), b"one");
        assert_eq!(tree.get(2).unwrap(), b"two");
        assert_eq!(tree.get(3).unwrap(), b"three");
    }

    #[test]
    fn test_custom_page_size() {
        let store = InMemoryPageStore::with_page_size(8192); // 8KB page size
        let mut tree = DataTree::new(store);

        // Test with larger data
        let large_data = vec![0u8; 4096];
        tree.put(1, &large_data).unwrap();
        assert_eq!(tree.get(1).unwrap(), large_data);
    }
}
