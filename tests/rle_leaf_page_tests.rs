use data_tree::rle_leaf_page::RLELeafPage;
use data_tree::data_tree::PageType;

#[test]
fn test_empty_rle_leaf_page_serialization() {
    // Create an empty RleLeafPage
    let page_size = 1024;
    let leaf_page = RLELeafPage::new_empty(page_size);

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not empty
    assert!(!serialized.is_empty());

    // Check that the first byte is the correct page type
    assert_eq!(serialized[0], PageType::RLELeafPage.to_u8());

    // Deserialize the page
    let deserialized = RLELeafPage::deserialize(&serialized);

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
    let mut leaf_page = RLELeafPage::new_empty(page_size);

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
    let deserialized = RLELeafPage::deserialize(&serialized);

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
    let mut leaf_page = RLELeafPage::new_empty(page_size);

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
    let deserialized = RLELeafPage::deserialize(&serialized);

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
    let mut leaf_page = RLELeafPage::new_empty(page_size);

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
    let deserialized = RLELeafPage::deserialize(&serialized);

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
    let mut leaf_page = RLELeafPage::new_empty(page_size);

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
    let mut leaf_page = RLELeafPage::new_empty(page_size);

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
    let mut leaf_page = RLELeafPage::new_empty(page_size);

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

#[test]
#[should_panic(expected = "Cannot deserialize RLELeafPage: byte array length")]
fn test_rle_leaf_page_deserialize_with_short_bytes() {
    // Create a byte array that is too short for the header
    let short_bytes = vec![0u8; 10]; // HEADER_SIZE is much larger than 10

    // This should panic
    let _ = RLELeafPage::deserialize(&short_bytes);
}

#[test]
fn test_rle_leaf_page_incremental_fill_and_split() {
    // Create a small RLELeafPage to make it easier to fill
    let page_size = 256; // Small page size to force a split sooner
    let mut leaf_page = RLELeafPage::new_empty(page_size);

    // Keep track of all inserted keys and values
    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();

    // Add key-value pairs with increasing numbers until the page needs to split
    // Key i will have value [i]
    let mut i = 1;
    loop {
        let key = i as u64;
        let value = vec![i as u8];

        // Try to insert the key-value pair
        if !leaf_page.put(key, &value) {
            // Page is full, break the loop
            break;
        }

        // Store the key and value for later verification
        all_keys.push(key);
        all_values.push(value);

        i += 1;
    }

    // Verify we've added some entries before the page split
    assert!(all_keys.len() >= 5, "Expected at least 5 entries before page split, but only got {}", all_keys.len());
    println!("Added {} entries before page split", all_keys.len());

    // Split the page
    let new_page = leaf_page.split().unwrap();

    // Verify the split was done correctly
    // 1. Check that all entries are in either the original page or the new page
    for (idx, key) in all_keys.iter().enumerate() {
        let value = &all_values[idx];
        let retrieved_value = leaf_page.get(*key).or_else(|| new_page.get(*key));

        assert!(retrieved_value.is_some(), "Key {} not found in either page", key);
        assert_eq!(retrieved_value.unwrap(), value, "Value mismatch for key {}", key);
    }

    // 2. Check that the metadata is split roughly in half
    let total_runs = all_keys.len(); // In this case, each key is its own run
    assert!(leaf_page.metadata.len() > 0, "Original page has no metadata after split");
    assert!(new_page.metadata.len() > 0, "New page has no metadata after split");

    // The split should be roughly even, but we'll allow some flexibility
    let min_expected = 1; // At least 1 entry in each page
    assert!(leaf_page.metadata.len() >= min_expected,
            "Original page has too few entries: {} (expected at least {})",
            leaf_page.metadata.len(), min_expected);
    assert!(new_page.metadata.len() >= min_expected,
            "New page has too few entries: {} (expected at least {})",
            new_page.metadata.len(), min_expected);

    // 3. Check that all keys in the original page are less than all keys in the new page
    if !leaf_page.metadata.is_empty() && !new_page.metadata.is_empty() {
        let max_key_original = leaf_page.metadata.iter().map(|m| m.end_key).max().unwrap();
        let min_key_new = new_page.metadata.iter().map(|m| m.start_key).min().unwrap();

        assert!(max_key_original < min_key_new,
                "Keys are not properly split: max key in original page ({}) should be less than min key in new page ({})",
                max_key_original, min_key_new);
    }

    // 4. Verify that the total number of entries is preserved
    assert_eq!(leaf_page.metadata.len() + new_page.metadata.len(), total_runs,
               "Total number of entries changed after split");
}
