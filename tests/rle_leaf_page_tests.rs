use data_tree::rle_leaf_page::RleLeafPage;
use data_tree::data_tree::PageType;

#[test]
fn test_empty_rle_leaf_page_serialization() {
    // Create an empty RleLeafPage
    let page_size = 1024;
    let leaf_page = RleLeafPage::new_empty(page_size);

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not empty
    assert!(!serialized.is_empty());

    // Check that the first byte is the correct page type
    assert_eq!(serialized[0], PageType::RleLeafPage.to_u8());

    // Deserialize the page
    let deserialized = RleLeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    assert_eq!(deserialized.metadata.len(), 0);
    assert_eq!(deserialized.data.len(), 0);
    assert_eq!(deserialized.prev_page_id, 0);
    assert_eq!(deserialized.next_page_id, 0);
}

#[test]
fn test_rle_leaf_page_with_single_key() {
    // Create a RleLeafPage with a single key
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new_empty(page_size);

    // Add a key-value pair
    let key = 3001u64;
    let value = b"test_value";
    assert!(leaf_page.put(key, value));

    // Verify the key can be retrieved
    let retrieved_value = leaf_page.get(key).unwrap();
    assert_eq!(retrieved_value, value);

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Deserialize the page
    let deserialized = RleLeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    assert_eq!(deserialized.metadata.len(), 1);
    assert!(deserialized.data.len() > 0);

    // Check that the key-value pair can be retrieved
    let retrieved_value = deserialized.get(key).unwrap();
    assert_eq!(retrieved_value, value);
}

#[test]
fn test_rle_leaf_page_with_run() {
    // Create a RleLeafPage with a run of keys with the same value
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new_empty(page_size);

    // Add a sequence of keys with the same value
    let start_key = 1000u64;
    let end_key = 1010u64;
    let value = b"same_value";

    for key in start_key..=end_key {
        assert!(leaf_page.put(key, value));
    }

    // Check that we have only one metadata entry (one run)
    assert_eq!(leaf_page.metadata.len(), 1);

    // Check that the run has the correct start and end keys
    assert_eq!(leaf_page.metadata[0].start_key, start_key);
    assert_eq!(leaf_page.metadata[0].end_key, end_key);

    // Verify all keys can be retrieved
    for key in start_key..=end_key {
        let retrieved_value = leaf_page.get(key).unwrap();
        assert_eq!(retrieved_value, value);
    }

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Deserialize the page
    let deserialized = RleLeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    assert_eq!(deserialized.metadata.len(), 1);

    // Verify all keys can still be retrieved after deserialization
    for key in start_key..=end_key {
        let retrieved_value = deserialized.get(key).unwrap();
        assert_eq!(retrieved_value, value);
    }
}

#[test]
fn test_rle_leaf_page_with_multiple_runs() {
    // Create a RleLeafPage with multiple runs
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new_empty(page_size);

    // Add first run
    let start_key1 = 1000u64;
    let end_key1 = 1005u64;
    let value1 = b"value1";

    for key in start_key1..=end_key1 {
        assert!(leaf_page.put(key, value1));
    }

    // Add second run
    let start_key2 = 2000u64;
    let end_key2 = 2005u64;
    let value2 = b"value2";

    for key in start_key2..=end_key2 {
        assert!(leaf_page.put(key, value2));
    }

    // Check that we have two metadata entries (two runs)
    assert_eq!(leaf_page.metadata.len(), 2);

    // Verify all keys from first run can be retrieved
    for key in start_key1..=end_key1 {
        let retrieved_value = leaf_page.get(key).unwrap();
        assert_eq!(retrieved_value, value1);
    }

    // Verify all keys from second run can be retrieved
    for key in start_key2..=end_key2 {
        let retrieved_value = leaf_page.get(key).unwrap();
        assert_eq!(retrieved_value, value2);
    }

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Deserialize the page
    let deserialized = RleLeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    assert_eq!(deserialized.metadata.len(), 2);

    // Verify all keys can still be retrieved after deserialization
    for key in start_key1..=end_key1 {
        let retrieved_value = deserialized.get(key).unwrap();
        assert_eq!(retrieved_value, value1);
    }

    for key in start_key2..=end_key2 {
        let retrieved_value = deserialized.get(key).unwrap();
        assert_eq!(retrieved_value, value2);
    }
}

#[test]
fn test_rle_leaf_page_insert_in_middle_of_run() {
    // Create a RleLeafPage with a run
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new_empty(page_size);

    // Add a run
    let start_key = 1000u64;
    let end_key = 1010u64;
    let value1 = b"value1";

    for key in start_key..=end_key {
        assert!(leaf_page.put(key, value1));
    }

    // Insert a different value in the middle of the run
    let middle_key = 1005u64;
    let value2 = b"value2";
    assert!(leaf_page.put(middle_key, value2));

    // Check that we now have three metadata entries (three runs)
    assert_eq!(leaf_page.metadata.len(), 3);

    // Verify the values
    for key in start_key..middle_key {
        let retrieved_value = leaf_page.get(key).unwrap();
        assert_eq!(retrieved_value, value1);
    }

    let retrieved_value = leaf_page.get(middle_key).unwrap();
    assert_eq!(retrieved_value, value2);

    for key in (middle_key+1)..=end_key {
        let retrieved_value = leaf_page.get(key).unwrap();
        assert_eq!(retrieved_value, value1);
    }
}

#[test]
fn test_rle_leaf_page_delete() {
    // Create a RleLeafPage with a run
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new_empty(page_size);

    // Add a run
    let start_key = 1000u64;
    let end_key = 1010u64;
    let value = b"value";

    for key in start_key..=end_key {
        assert!(leaf_page.put(key, value));
    }

    // Delete a key from the middle
    let middle_key = 1005u64;
    assert!(leaf_page.delete(middle_key));

    // Check that we now have two metadata entries (two runs)
    assert_eq!(leaf_page.metadata.len(), 2);

    // Verify the values
    for key in start_key..middle_key {
        let retrieved_value = leaf_page.get(key).unwrap();
        assert_eq!(retrieved_value, value);
    }

    assert!(leaf_page.get(middle_key).is_none());

    for key in (middle_key+1)..=end_key {
        let retrieved_value = leaf_page.get(key).unwrap();
        assert_eq!(retrieved_value, value);
    }
}

#[test]
fn test_rle_leaf_page_split() {
    // Create a RleLeafPage with multiple runs
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new_empty(page_size);

    // Add several runs
    let runs = vec![
        (1000u64, 1010u64, b"value1"),
        (2000u64, 2010u64, b"value2"),
        (3000u64, 3010u64, b"value3"),
        (4000u64, 4010u64, b"value4"),
    ];

    for (start, end, value) in &runs {
        for key in *start..=*end {
            assert!(leaf_page.put(key, *value));
        }
    }

    // Split the page
    let new_page = leaf_page.split().unwrap();

    // Check that the metadata is split between the two pages
    assert!(leaf_page.metadata.len() < runs.len());
    assert!(new_page.metadata.len() < runs.len());
    assert_eq!(leaf_page.metadata.len() + new_page.metadata.len(), runs.len());

    // Verify all keys can still be retrieved
    for (start, end, value) in &runs {
        for key in *start..=*end {
            let retrieved_value = leaf_page.get(key).or_else(|| new_page.get(key)).unwrap();
            assert_eq!(retrieved_value, *value);
        }
    }
}
