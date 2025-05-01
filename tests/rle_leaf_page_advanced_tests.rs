use data_tree::rle_leaf_page::RleLeafPage;
use data_tree::data_tree::PageType;

#[test]
fn test_rle_leaf_page_adjacent_runs_merge() {
    // Test that adjacent runs with the same value get merged
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add first run
    let value = b"same_value";
    for key in 1000..=1005 {
        assert!(leaf_page.insert(key, value));
    }

    // Add second run (adjacent to first)
    for key in 1006..=1010 {
        assert!(leaf_page.insert(key, value));
    }

    // Check that we have only one metadata entry (runs should be merged)
    assert_eq!(leaf_page.metadata.len(), 1);

    // Check that the run has the correct start and end keys
    assert_eq!(leaf_page.metadata[0].start_key, 1000);
    assert_eq!(leaf_page.metadata[0].end_key, 1010);

    // Verify all keys can be retrieved
    for key in 1000..=1010 {
        let retrieved_value = leaf_page.get(key).unwrap();
        assert_eq!(retrieved_value, value);
    }
}

#[test]
fn test_rle_leaf_page_value_deduplication() {
    // Test that identical values are stored only once
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add two runs with the same value but different keys
    let value = b"duplicate_value";

    // First run
    for key in 1000..=1005 {
        assert!(leaf_page.insert(key, value));
    }

    // Second run (not adjacent)
    for key in 2000..=2005 {
        assert!(leaf_page.insert(key, value));
    }

    // Check that we have two metadata entries (two runs)
    assert_eq!(leaf_page.metadata.len(), 2);

    // Check that both runs point to the same value offset
    assert_eq!(leaf_page.metadata[0].value_offset, leaf_page.metadata[1].value_offset);

    // Check that the value is stored only once
    assert_eq!(leaf_page.data.len(), value.len());
}

#[test]
fn test_rle_leaf_page_insert_at_run_boundaries() {
    // Test inserting at the boundaries of runs
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add a run
    let value1 = b"value1";
    for key in 1000..=1010 {
        assert!(leaf_page.insert(key, value1));
    }

    // Insert different values at the boundaries
    let value2 = b"value2";

    // Insert at start boundary
    assert!(leaf_page.insert(999, value2));

    // Insert at end boundary
    assert!(leaf_page.insert(1011, value2));

    // Check that we have three metadata entries
    assert_eq!(leaf_page.metadata.len(), 3);

    // Verify values
    assert_eq!(leaf_page.get(999).unwrap(), value2);
    for key in 1000..=1010 {
        assert_eq!(leaf_page.get(key).unwrap(), value1);
    }
    assert_eq!(leaf_page.get(1011).unwrap(), value2);
}

#[test]
fn test_rle_leaf_page_delete_entire_run() {
    // Test deleting an entire run
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add two runs
    let value1 = b"value1";
    for key in 1000..=1010 {
        assert!(leaf_page.insert(key, value1));
    }

    let value2 = b"value2";
    for key in 2000..=2010 {
        assert!(leaf_page.insert(key, value2));
    }

    // Delete all keys in the first run
    for key in 1000..=1010 {
        assert!(leaf_page.delete(key));
    }

    // Check that we have only one metadata entry left
    assert_eq!(leaf_page.metadata.len(), 1);

    // Verify the remaining run
    for key in 2000..=2010 {
        assert_eq!(leaf_page.get(key).unwrap(), value2);
    }

    // Verify the deleted keys are gone
    for key in 1000..=1010 {
        assert!(leaf_page.get(key).is_none());
    }
}

#[test]
fn test_rle_leaf_page_delete_at_run_boundaries() {
    // Test deleting at the boundaries of runs
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add a run
    let value = b"value";
    for key in 1000..=1010 {
        assert!(leaf_page.insert(key, value));
    }

    // Delete at start boundary
    assert!(leaf_page.delete(1000));

    // Delete at end boundary
    assert!(leaf_page.delete(1010));

    // Check that we still have one metadata entry
    assert_eq!(leaf_page.metadata.len(), 1);

    // Verify the run has been updated
    assert_eq!(leaf_page.metadata[0].start_key, 1001);
    assert_eq!(leaf_page.metadata[0].end_key, 1009);

    // Verify the deleted keys are gone
    assert!(leaf_page.get(1000).is_none());
    assert!(leaf_page.get(1010).is_none());

    // Verify the remaining keys
    for key in 1001..=1009 {
        assert_eq!(leaf_page.get(key).unwrap(), value);
    }
}

#[test]
fn test_rle_leaf_page_update_value_in_run() {
    // Test updating a value in the middle of a run
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add a run
    let value1 = b"value1";
    for key in 1000..=1010 {
        assert!(leaf_page.insert(key, value1));
    }

    // Update a value in the middle
    let middle_key = 1005;
    let value2 = b"value2";
    assert!(leaf_page.insert(middle_key, value2));

    // Check that we now have three metadata entries
    assert_eq!(leaf_page.metadata.len(), 3);

    // Verify the values
    for key in 1000..middle_key {
        assert_eq!(leaf_page.get(key).unwrap(), value1);
    }
    assert_eq!(leaf_page.get(middle_key).unwrap(), value2);
    for key in (middle_key+1)..=1010 {
        assert_eq!(leaf_page.get(key).unwrap(), value1);
    }
}

#[test]
fn test_rle_leaf_page_update_multiple_values() {
    // Test updating multiple values in a run
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add a run
    let value1 = b"value1";
    for key in 1000..=1010 {
        assert!(leaf_page.insert(key, value1));
    }

    // Update a range of values in the middle
    let value2 = b"value2";
    for key in 1003..=1007 {
        assert!(leaf_page.insert(key, value2));
    }

    // We should have at least 3 metadata entries, but could have more
    // depending on how the implementation handles consecutive updates
    assert!(leaf_page.metadata.len() >= 3);

    // Verify the values
    for key in 1000..=1002 {
        assert_eq!(leaf_page.get(key).unwrap(), value1);
    }
    for key in 1003..=1007 {
        assert_eq!(leaf_page.get(key).unwrap(), value2);
    }
    for key in 1008..=1010 {
        assert_eq!(leaf_page.get(key).unwrap(), value1);
    }
}

#[test]
fn test_rle_leaf_page_compact_data() {
    // Test that data gets compacted when needed
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add several runs with different values
    let runs = vec![
        (1000..=1010, b"value1"),
        (2000..=2010, b"value2"),
        (3000..=3010, b"value3"),
    ];

    for (range, value) in &runs {
        for key in range.clone() {
            assert!(leaf_page.insert(key, *value));
        }
    }

    // Record the initial data size
    let initial_data_size = leaf_page.data.len();

    // Delete the second run
    for key in 2000..=2010 {
        assert!(leaf_page.delete(key));
    }

    // Force compaction
    leaf_page.compact_data();

    // Check that the data size has decreased
    assert!(leaf_page.data.len() < initial_data_size);

    // Verify the remaining runs
    for key in 1000..=1010 {
        assert_eq!(leaf_page.get(key).unwrap(), b"value1");
    }
    for key in 3000..=3010 {
        assert_eq!(leaf_page.get(key).unwrap(), b"value3");
    }
}

#[test]
fn test_rle_leaf_page_is_full() {
    // Test the is_full method
    let page_size = 100; // Small page size to make it easier to fill
    let mut leaf_page = RleLeafPage::new(page_size);

    // Create a value that's small enough to fit initially
    let value = b"test";

    // Insert the value a few times
    for i in 0..5 {
        assert!(leaf_page.insert(i, value));
    }

    // Create a value that's too large to fit
    let large_value = vec![b'x'; page_size]; // Definitely too large

    // Verify that is_full returns true for the large value
    assert!(leaf_page.is_full(&large_value));

    // Try to add the large value (should fail)
    assert!(!leaf_page.insert(100, &large_value));
}

#[test]
fn test_rle_leaf_page_serialization_format() {
    // Test the serialization format
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Add a run
    let value = b"test_value";
    for key in 1000..=1010 {
        assert!(leaf_page.insert(key, value));
    }

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check the format of the serialized data
    // First byte is the page type
    assert_eq!(serialized[0], PageType::RleLeafPage.to_u8());

    // Next 8 bytes are the metadata count (1 in this case)
    let metadata_count = u64::from_le_bytes(serialized[1..9].try_into().unwrap());
    assert_eq!(metadata_count, 1);

    // Next 8 bytes are the data start offset
    let data_start = u64::from_le_bytes(serialized[9..17].try_into().unwrap()) as usize;
    assert!(data_start > 0);

    // Deserialize the page
    let deserialized = RleLeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    assert_eq!(deserialized.metadata.len(), 1);
    assert_eq!(deserialized.metadata[0].start_key, 1000);
    assert_eq!(deserialized.metadata[0].end_key, 1010);

    // Verify all keys can be retrieved
    for key in 1000..=1010 {
        assert_eq!(deserialized.get(key).unwrap(), value);
    }
}

#[test]
fn test_rle_leaf_page_with_large_data() {
    // Test with large values
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Create a large value
    let value = vec![b'x'; 500]; // 500 bytes of 'x'

    // Add a run with the large value
    for key in 1000..=1005 {
        assert!(leaf_page.insert(key, &value));
    }

    // Check that we have one metadata entry
    assert_eq!(leaf_page.metadata.len(), 1);

    // Verify all keys can be retrieved
    for key in 1000..=1005 {
        assert_eq!(leaf_page.get(key).unwrap(), value);
    }

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not larger than the page size
    assert!(serialized.len() <= page_size);

    // Deserialize the page
    let deserialized = RleLeafPage::deserialize(&serialized);

    // Verify all keys can still be retrieved
    for key in 1000..=1005 {
        assert_eq!(deserialized.get(key).unwrap(), value);
    }
}

#[test]
fn test_rle_leaf_page_with_max_data() {
    // Test with maximum data that can fit in a page
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Calculate how much data we can fit
    let max_value_size = leaf_page.max_value_size();
    let value = vec![b'y'; max_value_size]; // Fill with 'y'

    // Add a key with the maximum value size
    assert!(leaf_page.insert(1000, &value));

    // Verify the key can be retrieved
    assert_eq!(leaf_page.get(1000).unwrap(), value);

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not larger than the page size
    assert!(serialized.len() <= page_size);

    // Deserialize the page
    let deserialized = RleLeafPage::deserialize(&serialized);

    // Verify the key can still be retrieved
    assert_eq!(deserialized.get(1000).unwrap(), value);
}

#[test]
fn test_rle_leaf_page_with_linked_pages() {
    // Test with linked pages
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // Set prev and next page IDs
    let prev_page_id = 123;
    let next_page_id = 456;
    leaf_page.set_prev_page_id(prev_page_id);
    leaf_page.set_next_page_id(next_page_id);

    // Add a run
    let value = b"test_value";
    for key in 1000..=1010 {
        assert!(leaf_page.insert(key, value));
    }

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Deserialize the page
    let deserialized = RleLeafPage::deserialize(&serialized);

    // Check that the page IDs are preserved
    assert_eq!(deserialized.prev_page_id(), prev_page_id);
    assert_eq!(deserialized.next_page_id(), next_page_id);
}

#[test]
fn test_rle_leaf_page_complex_operations() {
    // Test a complex sequence of operations
    let page_size = 1024;
    let mut leaf_page = RleLeafPage::new(page_size);

    // 1. Add several runs
    let value1 = b"value1";
    let value2 = b"value2";
    let value3 = b"value3";

    // First run
    for key in 1000..=1010 {
        assert!(leaf_page.insert(key, value1));
    }

    // Second run
    for key in 2000..=2010 {
        assert!(leaf_page.insert(key, value2));
    }

    // Third run
    for key in 3000..=3010 {
        assert!(leaf_page.insert(key, value3));
    }

    // 2. Update some values
    for key in 1005..=1007 {
        assert!(leaf_page.insert(key, value2));
    }

    // 3. Delete some keys
    assert!(leaf_page.delete(1000));
    assert!(leaf_page.delete(2005));
    assert!(leaf_page.delete(3010));

    // 4. Add more keys
    for key in 4000..=4005 {
        assert!(leaf_page.insert(key, value1));
    }

    // 5. Verify all operations
    // First run should be split
    assert!(leaf_page.get(1000).is_none());
    for key in 1001..=1004 {
        assert_eq!(leaf_page.get(key).unwrap(), value1);
    }
    for key in 1005..=1007 {
        assert_eq!(leaf_page.get(key).unwrap(), value2);
    }
    for key in 1008..=1010 {
        assert_eq!(leaf_page.get(key).unwrap(), value1);
    }

    // Second run should have a gap
    for key in 2000..=2004 {
        assert_eq!(leaf_page.get(key).unwrap(), value2);
    }
    assert!(leaf_page.get(2005).is_none());
    for key in 2006..=2010 {
        assert_eq!(leaf_page.get(key).unwrap(), value2);
    }

    // Third run should be shorter
    for key in 3000..=3009 {
        assert_eq!(leaf_page.get(key).unwrap(), value3);
    }
    assert!(leaf_page.get(3010).is_none());

    // Fourth run should be intact
    for key in 4000..=4005 {
        assert_eq!(leaf_page.get(key).unwrap(), value1);
    }

    // 6. Serialize and deserialize
    let serialized = leaf_page.serialize();
    let deserialized = RleLeafPage::deserialize(&serialized);

    // 7. Verify everything is still correct after deserialization
    assert!(deserialized.get(1000).is_none());
    for key in 1001..=1004 {
        assert_eq!(deserialized.get(key).unwrap(), value1);
    }
    for key in 1005..=1007 {
        assert_eq!(deserialized.get(key).unwrap(), value2);
    }
    for key in 1008..=1010 {
        assert_eq!(deserialized.get(key).unwrap(), value1);
    }

    for key in 2000..=2004 {
        assert_eq!(deserialized.get(key).unwrap(), value2);
    }
    assert!(deserialized.get(2005).is_none());
    for key in 2006..=2010 {
        assert_eq!(deserialized.get(key).unwrap(), value2);
    }

    for key in 3000..=3009 {
        assert_eq!(deserialized.get(key).unwrap(), value3);
    }
    assert!(deserialized.get(3010).is_none());

    for key in 4000..=4005 {
        assert_eq!(deserialized.get(key).unwrap(), value1);
    }
}
