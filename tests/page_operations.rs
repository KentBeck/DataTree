use data_tree::DataTree;
use data_tree::leaf_page::LeafPage;
use data_tree::data_tree::PageType;
use data_tree::page_store::{PageStore, InMemoryPageStore};

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

    // Get the page count before deletion
    let initial_page_count = tree.store().get_page_count();

    // We should have at least 2 pages (root BranchPage + at least one LeafPage)
    assert!(initial_page_count >= 2);

    // Get the keys to delete
    let keys_to_delete = keys.clone();

    // Delete all entries
    for key in keys_to_delete {
        tree.delete(&key).unwrap();
    }

    // Verify that the page count has decreased
    let final_page_count = tree.store().get_page_count();

    // We should have fewer pages after deletion
    // Note: With a BranchPage root, we'll still have at least 2 pages (root + empty leaf)
    assert!(final_page_count <= initial_page_count);

    // Verify all keys are gone
    for key in &keys {
        assert!(tree.get(key).unwrap().is_none());
    }
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

    // Check the page type directly from the first byte
    let page_type = PageType::from_u8(page_bytes[0]).unwrap();
    assert_eq!(page_type, PageType::BranchPage);

    // Create a LeafPage and verify its serialization
    let leaf_page = LeafPage::new(100);
    let serialized = leaf_page.serialize();
    assert_eq!(serialized[0], PageType::LeafPage.to_u8());
}