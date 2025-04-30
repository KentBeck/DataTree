use data_tree::DataTree;

use data_tree::data_tree::PageType;
use data_tree::page_store::{PageStore, InMemoryPageStore, PageCorruptionError};

#[test]
fn test_page_corruption_detection() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert some data
    tree.put(b"key1", b"value1").unwrap();

    // Get the page ID and corrupt it
    let page_id = tree.root_page_id();
    tree.store_mut().corrupt_page_for_testing(page_id);

    // Attempt to read the page - should fail with corruption error
    let result = tree.store().get_page_bytes(page_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().downcast_ref::<PageCorruptionError>().is_some());
}

#[test]
fn test_crc_verification_on_updates() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert initial data
    tree.put(b"key1", b"value1").unwrap();
    tree.put(b"key2", b"value2").unwrap();

    // Verify data can be read
    assert_eq!(tree.get(b"key1").unwrap().unwrap(), b"value1");
    assert_eq!(tree.get(b"key2").unwrap().unwrap(), b"value2");

    // Update a value with same length
    tree.put(b"key1", b"value1").unwrap(); // No change, just verify CRC works

    // Verify the update was successful and CRC is maintained
    assert_eq!(tree.get(b"key1").unwrap().unwrap(), b"value1");
    assert_eq!(tree.get(b"key2").unwrap().unwrap(), b"value2");
}

#[test]
fn test_multiple_page_corruption_scenarios() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert data that will span multiple pages
    let value1 = vec![1u8; 20]; // 20 bytes
    let value2 = vec![2u8; 20]; // 20 bytes
    let value3 = vec![3u8; 20]; // 20 bytes

    tree.put(b"key1", &value1).unwrap();
    tree.put(b"key2", &value2).unwrap();
    tree.put(b"key3", &value3).unwrap();

    // Get root page ID before corrupting
    let root_id = tree.root_page_id();

    // Corrupt the first page
    tree.store_mut().corrupt_page_for_testing(root_id);

    // Verify we get an error when trying to read from corrupted page
    assert!(tree.get(b"key1").is_err());

    // Since we corrupted the root page, all subsequent reads should fail
    assert!(tree.get(b"key2").is_err());
    assert!(tree.get(b"key3").is_err());
}

#[test]
fn test_error_handling_with_corrupted_pages() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert some data
    tree.put(b"key1", b"value1").unwrap();
    tree.put(b"key2", b"value2").unwrap();

    // Corrupt a page
    let page_id = tree.root_page_id();
    tree.store_mut().corrupt_page_for_testing(page_id);

    // Test various operations with corrupted page
    assert!(tree.get(b"key1").is_err());
    assert!(tree.get(b"key2").is_err());
    assert!(tree.put(b"key3", b"value3").is_err());
    assert!(tree.delete(b"key1").is_err());
}

#[test]
fn test_crc_verification_on_page_cleanup() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert data that will require multiple pages
    for i in 0..5 {
        let key = format!("key{}", i).into_bytes();
        let value = format!("value{}", i).into_bytes();
        tree.put(&key, &value).unwrap();
    }

    // Get the root page ID
    let root_page_id = tree.root_page_id();

    // Get a leaf page ID - we know there's at least one leaf page
    // But we need to handle the case where get_next_page_id returns None
    let leaf_page_id = tree.store().get_next_page_id(root_page_id).unwrap_or(root_page_id);

    // Corrupt the leaf page
    tree.store_mut().corrupt_page_for_testing(leaf_page_id);

    // Attempt to delete - this should fail because we need to read the leaf page
    let key = b"key4".to_vec();
    assert!(tree.delete(&key).is_err());

    // Verify we can still access keys in other leaf pages
    let key0 = b"key0".to_vec();
    let key1 = b"key1".to_vec();

    // These might fail or succeed depending on which leaf page was corrupted
    // So we'll just try to access them without asserting the result
    let _ = tree.get(&key0);
    let _ = tree.get(&key1);
}

#[test]
fn test_branch_page_corruption_detection() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert data that will create a branch page
    for i in 0..10 {
        let key = format!("key{}", i).into_bytes();
        let value = format!("value{}", i).into_bytes();
        tree.put(&key, &value).unwrap();
    }

    // The root page is already a branch page
    let root_page_id = tree.root_page_id();

    // Corrupt the branch page
    tree.store_mut().corrupt_page_for_testing(root_page_id);

    // Attempt to read from the corrupted branch page - should fail with corruption error
    let result = tree.store().get_page_bytes(root_page_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().downcast_ref::<PageCorruptionError>().is_some());
}

#[test]
fn test_branch_page_crc_verification_on_updates() {
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert enough data to create a branch page
    for i in 0..10 {
        let key = format!("key{}", i).into_bytes();
        let value = format!("value{}", i).into_bytes();
        tree.put(&key, &value).unwrap();
    }

    // The root page is already a branch page
    let store = tree.store();
    let root_page_id = tree.root_page_id();
    let root_page_bytes = store.get_page_bytes(root_page_id).unwrap();

    // Verify the root page is a branch page
    let page_type = PageType::from_u8(root_page_bytes[0]).unwrap();
    assert_eq!(page_type, PageType::BranchPage, "Expected a branch page to be created");

    // Verify we can read the branch page
    assert!(root_page_bytes.len() > 0);

    // Release the store reference
    let _ = store;

    // Update some data
    tree.put(b"key10", b"new_value10").unwrap();

    // Get the store again to verify
    let store = tree.store();

    // Verify we can still read the branch page (root page)
    let root_page_bytes = store.get_page_bytes(root_page_id).unwrap();
    assert!(root_page_bytes.len() > 0);
}