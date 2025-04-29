use crate::page_store::{PageStore, InMemoryPageStore};
use crate::data_tree::DataTree;

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

#[test]
fn test_exact_page_size() {
    let store = InMemoryPageStore::new();
    let page_size = store.page_size();
    let mut tree = DataTree::new(store);
    let meta_size = 24; // 8 bytes for key + 8 bytes for offset + 8 bytes for length
    let header_size = 24; // 8 bytes for count + 8 bytes for data_start + 8 bytes for used_bytes
    let data = vec![0; page_size - header_size - meta_size];
    tree.put(1, &data).unwrap();
    assert_eq!(tree.get(1).unwrap(), data);
}

#[test]
fn test_zero_length_data() {
    let store = InMemoryPageStore::new();
    let mut tree = DataTree::new(store);
    tree.put(1, &[]).unwrap();
    assert_eq!(tree.get(1).unwrap(), &[]);
}

#[test]
fn test_data_integrity() {
    let store = InMemoryPageStore::new();
    let mut tree = DataTree::new(store);
    let data = (0..255).collect::<Vec<u8>>();
    tree.put(1, &data).unwrap();
    let retrieved = tree.get(1).unwrap();
    assert_eq!(retrieved.len(), data.len());
    assert!(retrieved.iter().zip(data.iter()).all(|(a, b)| a == b));
}

#[test]
fn test_sequential_keys() {
    let store = InMemoryPageStore::new();
    let mut tree = DataTree::new(store);
    for i in 0..10 {
        let data = &[i as u8];
        tree.put(i as u64, data).unwrap();
        assert_eq!(tree.get(i as u64).unwrap(), data);
    }
}

#[test]
fn test_large_keys() {
    let store = InMemoryPageStore::new();
    let mut tree = DataTree::new(store);
    let large_key = u64::MAX;
    tree.put(large_key, &[1, 2, 3]).unwrap();
    assert_eq!(tree.get(large_key).unwrap(), &[1, 2, 3]);
}

#[test]
fn test_update_with_larger_value() {
    let store = InMemoryPageStore::new();
    let mut tree = DataTree::new(store);
    tree.put(1, &[1, 2]).unwrap();
    tree.put(1, &[1, 2, 3, 4]).unwrap();
    assert_eq!(tree.get(1).unwrap(), &[1, 2, 3, 4]);
}

#[test]
fn test_update_with_smaller_value() {
    let store = InMemoryPageStore::new();
    let mut tree = DataTree::new(store);
    tree.put(1, &[1, 2, 3, 4]).unwrap();
    tree.put(1, &[1, 2]).unwrap();
    assert_eq!(tree.get(1).unwrap(), &[1, 2]);
}

#[test]
fn test_data_tree_with_serialization() {
    let store = InMemoryPageStore::new();
    let mut tree = DataTree::new(store);
    
    // Insert data
    tree.put(1, &[1, 2, 3]).unwrap();
    tree.put(2, &[4, 5, 6]).unwrap();
    
    // Verify data
    assert_eq!(tree.get(1).unwrap(), vec![1, 2, 3]);
    assert_eq!(tree.get(2).unwrap(), vec![4, 5, 6]);
    
    // Update data
    tree.put(1, &[7, 8, 9]).unwrap();
    assert_eq!(tree.get(1).unwrap(), vec![7, 8, 9]);
    
    // Delete data
    tree.delete(2).unwrap();
    assert!(tree.get(2).is_err());
} 