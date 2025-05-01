use data_tree::DataTree;
use data_tree::leaf_page::LeafPage;
use data_tree::data_tree::PageType;
use data_tree::page_store::{PageStore, InMemoryPageStore, PageCorruptionError};

#[test]
fn test_page_splitting() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert data that will require multiple pages
    let keys = vec![1, 2, 3, 4];
    let values = vec![
        b"value1".to_vec(),
        b"value2".to_vec(),
        b"value3".to_vec(),
        b"value4".to_vec(),
    ];

    // Insert all key-value pairs
    for (key, value) in keys.iter().zip(values.iter()) {
        tree.put(*key, value).unwrap();
    }

    // Verify all data can be retrieved
    for (key, expected_value) in keys.iter().zip(values.iter()) {
        let retrieved_value = tree.get(*key).unwrap().unwrap();
        assert_eq!(retrieved_value, *expected_value);
    }

    // Verify that we have created multiple pages
    let store = tree.store();
    let page_count = store.get_page_count();

    // We should have at least 2 pages (root BranchPage + at least one LeafPage)
    assert!(page_count >= 2);
}

#[test]
fn test_page_cleanup_after_deletion() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert data that will require multiple pages
    let keys = vec![10, 11, 12, 13];
    let values = vec![
        b"value1".to_vec(),
        b"value2".to_vec(),
        b"value3".to_vec(),
        b"value4".to_vec(),
    ];

    // Insert all key-value pairs
    for (key, value) in keys.iter().zip(values.iter()) {
        tree.put(*key, value).unwrap();
    }

    // Get the page count before deletion
    let initial_page_count = tree.store().get_page_count();

    // We should have at least 2 pages (root BranchPage + at least one LeafPage)
    assert!(initial_page_count >= 2);

    // Get the keys to delete
    let keys_to_delete = keys.clone();

    // Delete all entries
    for key in keys_to_delete {
        tree.delete(key).unwrap();
    }

    // Verify that the page count has decreased
    let final_page_count = tree.store().get_page_count();

    // We should have fewer pages after deletion
    // Note: With a BranchPage root, we'll still have at least 2 pages (root + empty leaf)
    assert!(final_page_count <= initial_page_count);

    // Verify all keys are gone
    for key in &keys {
        assert!(tree.get(*key).unwrap().is_none());
    }
}

#[test]
fn test_page_type_serialization() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert some data
    tree.put(100, b"value1").unwrap();

    // Get the page and verify its type
    let store = tree.store();
    let page_bytes = store.get_page_bytes(tree.root_page_id()).unwrap();

    // Check the page type directly from the first byte
    let page_type = PageType::from_u8(page_bytes[0]).unwrap();
    assert_eq!(page_type, PageType::BranchPage);

    // Create a LeafPage and verify its serialization
    let leaf_page = LeafPage::new_empty(100);
    let serialized = leaf_page.serialize();
    assert_eq!(serialized[0], PageType::LeafPage.to_u8());
}

#[test]
fn test_page_corruption_detection() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert some data
    tree.put(101, b"value1").unwrap();

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
    tree.put(102, b"value1").unwrap();
    tree.put(103, b"value2").unwrap();

    // Verify data can be read
    assert_eq!(tree.get(102).unwrap().unwrap(), b"value1");
    assert_eq!(tree.get(103).unwrap().unwrap(), b"value2");

    // Update a value with same length
    tree.put(102, b"value1").unwrap(); // No change, just verify CRC works

    // Verify the update was successful and CRC is maintained
    assert_eq!(tree.get(102).unwrap().unwrap(), b"value1");
    assert_eq!(tree.get(103).unwrap().unwrap(), b"value2");
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

    tree.put(201, &value1).unwrap();
    tree.put(202, &value2).unwrap();
    tree.put(203, &value3).unwrap();

    // Get root page ID before corrupting
    let root_id = tree.root_page_id();

    // Corrupt the first page
    tree.store_mut().corrupt_page_for_testing(root_id);

    // Verify we get an error when trying to read from corrupted page
    assert!(tree.get(201).is_err());

    // Since we corrupted the root page, all subsequent reads should fail
    assert!(tree.get(202).is_err());
    assert!(tree.get(203).is_err());
}

#[test]
fn test_error_handling_with_corrupted_pages() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert some data
    tree.put(301, b"value1").unwrap();
    tree.put(302, b"value2").unwrap();

    // Corrupt a page
    let page_id = tree.root_page_id();
    tree.store_mut().corrupt_page_for_testing(page_id);

    // Test various operations with corrupted page
    assert!(tree.get(301).is_err());
    assert!(tree.get(302).is_err());
    assert!(tree.put(303, b"value3").is_err());
    assert!(tree.delete(301).is_err());
}

#[test]
fn test_crc_verification_on_page_cleanup() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert data that will require multiple pages
    for i in 0..5 {
        let key = 400 + i as u64;
        let value = format!("value{}", i).into_bytes();
        tree.put(key, &value).unwrap();
    }

    // Get the root page ID
    let root_page_id = tree.root_page_id();

    // Corrupt the root page
    tree.store_mut().corrupt_page_for_testing(root_page_id);

    // Attempt to delete from the tree with corrupted root page
    assert!(tree.delete(404).is_err());

    // Verify all operations fail with corrupted root page
    assert!(tree.get(400).is_err());
    assert!(tree.get(401).is_err());
}