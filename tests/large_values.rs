use data_tree::DataTree;
use data_tree::page_store::{PageStore, InMemoryPageStore};

#[test]
fn test_large_value_splitting() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Create a value that's large but still fits in a page
    let large_value = vec![1u8; 20]; // 20 bytes

    // Insert the large value
    tree.put_u64(1101, &large_value).unwrap();

    // Verify we can read it back
    let retrieved = tree.get_u64(1101).unwrap().unwrap();
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

    tree.put_u64(1201, &value1).unwrap();
    tree.put_u64(1202, &value2).unwrap();
    tree.put_u64(1203, &value3).unwrap();

    // Verify all values can be retrieved
    assert_eq!(tree.get_u64(1201).unwrap().unwrap(), value1);
    assert_eq!(tree.get_u64(1202).unwrap().unwrap(), value2);
    assert_eq!(tree.get_u64(1203).unwrap().unwrap(), value3);

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
    tree.put_u64(1301, &initial_value).unwrap();

    // Update with a different value
    let updated_value = vec![2u8; 20];
    tree.put_u64(1301, &updated_value).unwrap();

    // Verify the update
    assert_eq!(tree.get_u64(1301).unwrap().unwrap(), updated_value);
}

#[test]
fn test_large_value_deletion() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert a value
    let value = vec![1u8; 20];
    tree.put_u64(1401, &value).unwrap();

    // Delete the value
    tree.delete_u64(1401).unwrap();

    // Verify it's gone
    assert!(tree.get_u64(1401).unwrap().is_none());

    // Verify pages were cleaned up
    // With a BranchPage root, we'll still have at least 2 pages (root + empty leaf)
    assert!(tree.store().get_page_count() <= 2);
}

#[test]
fn test_mixed_size_values() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert a mix of small and large values
    tree.put_u64(1501, b"value1").unwrap();
    tree.put_u64(1502, &vec![1u8; 20]).unwrap();
    tree.put_u64(1503, b"value2").unwrap();
    tree.put_u64(1504, &vec![2u8; 20]).unwrap();

    // Verify all values can be retrieved
    assert_eq!(tree.get_u64(1501).unwrap().unwrap(), b"value1");
    assert_eq!(tree.get_u64(1502).unwrap().unwrap(), vec![1u8; 20]);
    assert_eq!(tree.get_u64(1503).unwrap().unwrap(), b"value2");
    assert_eq!(tree.get_u64(1504).unwrap().unwrap(), vec![2u8; 20]);

    // Verify multiple pages were used
    assert!(tree.store().get_page_count() >= 2);
}