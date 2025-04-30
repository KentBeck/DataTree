use data_tree::{DataTree, LeafPage, PageType, PageStore};
use data_tree::page_store::{InMemoryPageStore, PageCorruptionError};

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
    let mut current_page_id = tree.root_page_id();
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
    let mut page_ids = Vec::new();
    let mut current_page_id = tree.root_page_id();
    
    {
        let store = tree.store();
        while let Some(next_page_id) = store.get_next_page_id(current_page_id) {
            page_ids.push(current_page_id);
            current_page_id = next_page_id;
        }
        page_ids.push(current_page_id);
    }

    // We should have at least 2 pages
    assert!(page_ids.len() >= 2);
    
    // Delete all entries from the last page
    let last_page_id = *page_ids.last().unwrap();
    let keys_to_delete = {
        let store = tree.store();
        let last_page_bytes = store.get_page_bytes(last_page_id).unwrap();
        let last_page = LeafPage::deserialize(&last_page_bytes);
        
        // Collect keys to delete
        last_page.metadata().iter().map(|meta| {
            let key = &last_page.data()[meta.key_offset..meta.key_offset + meta.key_length];
            key.to_vec()
        }).collect::<Vec<_>>()
    };

    // Delete all entries
    for key in keys_to_delete {
        tree.delete(&key).unwrap();
    }

    // Verify the page no longer exists
    let store = tree.store();
    assert!(!store.page_exists(last_page_id));
    
    // Verify the page is no longer in the linked list
    let mut current_page_id = tree.root_page_id();
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

#[test]
fn test_page_type_serialization() {
    // Create store with 100 byte pages
    let store = InMemoryPageStore::with_page_size(100);
    let mut tree = DataTree::new(store);

    // Insert some data
    tree.put(b"key1", b"value1").unwrap();

    // Get the page and verify its type
    let store = tree.store();
    let page_bytes = store.get_page_bytes(tree.root_page_id()).unwrap();
    let page = LeafPage::deserialize(&page_bytes);
    
    assert_eq!(page.page_type(), PageType::LeafPage);

    // Verify the page type is correctly serialized
    let serialized = page.serialize();
    assert_eq!(serialized[0], PageType::LeafPage.to_u8());
}

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

    // Get the last page ID
    let mut current_page_id = tree.root_page_id();
    let mut last_page_id = current_page_id;
    while let Some(next_page_id) = tree.store().get_next_page_id(current_page_id) {
        last_page_id = current_page_id;
        current_page_id = next_page_id;
    }

    // Corrupt the last page
    tree.store_mut().corrupt_page_for_testing(last_page_id);

    // Attempt to delete from the corrupted page
    assert!(tree.delete(b"key4").is_err());

    // Verify other pages are still accessible
    assert_eq!(tree.get(b"key0").unwrap().unwrap(), b"value0");
    assert_eq!(tree.get(b"key1").unwrap().unwrap(), b"value1");
} 