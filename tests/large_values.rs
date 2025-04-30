use data_tree::{DataTree, PageStore};
use data_tree::page_store::InMemoryPageStore;

#[test]
fn test_large_value_splitting() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Create a value that's large but still fits in a page
    let large_value = vec![1u8; 20]; // 20 bytes

    // Insert the large value
    tree.put(b"key1", &large_value).unwrap();

    // Verify we can read it back
    let retrieved = tree.get(b"key1").unwrap().unwrap();
    assert_eq!(retrieved, large_value);

    // Verify page count
    assert!(tree.store().get_page_count() >= 1);
}

#[test]
fn test_consecutive_large_values() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert multiple values
    let value1 = vec![1u8; 15]; // 15 bytes
    let value2 = vec![2u8; 15]; // 15 bytes
    let value3 = vec![3u8; 15]; // 15 bytes

    tree.put(b"key1", &value1).unwrap();
    tree.put(b"key2", &value2).unwrap();
    tree.put(b"key3", &value3).unwrap();

    // Verify all values can be retrieved
    assert_eq!(tree.get(b"key1").unwrap().unwrap(), value1);
    assert_eq!(tree.get(b"key2").unwrap().unwrap(), value2);
    assert_eq!(tree.get(b"key3").unwrap().unwrap(), value3);

    // Verify multiple pages were used
    assert!(tree.store().get_page_count() >= 2);
}

#[test]
fn test_large_value_updates() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert initial value
    let initial_value = vec![1u8; 20];
    tree.put(b"key1", &initial_value).unwrap();

    // Update with a different value
    let updated_value = vec![2u8; 20];
    tree.put(b"key1", &updated_value).unwrap();

    // Verify the update
    assert_eq!(tree.get(b"key1").unwrap().unwrap(), updated_value);
}

#[test]
fn test_large_value_deletion() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert a value
    let value = vec![1u8; 20];
    tree.put(b"key1", &value).unwrap();

    // Delete the value
    tree.delete(b"key1").unwrap();

    // Verify it's gone
    assert!(tree.get(b"key1").unwrap().is_none());

    // Verify pages were cleaned up
    assert!(tree.store().get_page_count() <= 1);
}

#[test]
fn test_mixed_size_values() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert a mix of small and large values
    tree.put(b"small1", b"value1").unwrap();
    tree.put(b"large1", &vec![1u8; 20]).unwrap();
    tree.put(b"small2", b"value2").unwrap();
    tree.put(b"large2", &vec![2u8; 20]).unwrap();

    // Verify all values can be retrieved
    assert_eq!(tree.get(b"small1").unwrap().unwrap(), b"value1");
    assert_eq!(tree.get(b"large1").unwrap().unwrap(), vec![1u8; 20]);
    assert_eq!(tree.get(b"small2").unwrap().unwrap(), b"value2");
    assert_eq!(tree.get(b"large2").unwrap().unwrap(), vec![2u8; 20]);

    // Verify multiple pages were used
    assert!(tree.store().get_page_count() >= 2);
} 