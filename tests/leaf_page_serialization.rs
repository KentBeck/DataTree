use data_tree::leaf_page::LeafPage;
use data_tree::data_tree::PageType;

#[test]
fn test_empty_leaf_page_serialization() {
    // Create an empty LeafPage
    let page_size = 1024;
    let leaf_page = LeafPage::new_empty(page_size);

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not empty
    assert!(!serialized.is_empty());

    // Check that the first byte is the correct page type
    assert_eq!(serialized[0], PageType::LeafPage.to_u8());

    // Deserialize the page
    let deserialized = LeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    // Note: page_size is set to the length of the serialized data, not the original page_size
    assert_eq!(deserialized.metadata.len(), 0);
    assert_eq!(deserialized.data.len(), 0);
    assert_eq!(deserialized.prev_page_id, 0);
    assert_eq!(deserialized.next_page_id, 0);
}

#[test]
fn test_leaf_page_with_single_entry() {
    // Create a LeafPage with a single entry
    let page_size = 1024;
    let mut leaf_page = LeafPage::new_with_size(page_size);

    // Add a key-value pair
    let key = 3001u64;
    let value = b"test_value";
    assert!(leaf_page.put(key, value));

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not empty
    assert!(!serialized.is_empty());

    // Check that the first byte is the correct page type
    assert_eq!(serialized[0], PageType::LeafPage.to_u8());

    // Deserialize the page
    let deserialized = LeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    // Note: page_size is set to the length of the serialized data, not the original page_size
    assert_eq!(deserialized.metadata.len(), 1);
    assert!(deserialized.data.len() > 0);
    assert_eq!(deserialized.prev_page_id, 0);
    assert_eq!(deserialized.next_page_id, 0);

    // Check that the key-value pair can be retrieved
    let retrieved_value = deserialized.get(key).unwrap();
    assert_eq!(retrieved_value, value);
}

#[test]
fn test_leaf_page_with_multiple_entries() {
    // Create a LeafPage with multiple entries
    let page_size = 1024;
    let mut leaf_page = LeafPage::new_with_size(page_size);

    // Add several key-value pairs
    let entries = vec![
        (3101u64, b"value1"),
        (3102u64, b"value2"),
        (3103u64, b"value3"),
    ];

    for (key, value) in &entries {
        assert!(leaf_page.put(*key, *value));
    }

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not empty
    assert!(!serialized.is_empty());

    // Check that the first byte is the correct page type
    assert_eq!(serialized[0], PageType::LeafPage.to_u8());

    // Deserialize the page
    let deserialized = LeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    // Note: page_size is set to the length of the serialized data, not the original page_size
    assert_eq!(deserialized.metadata.len(), entries.len());
    assert!(deserialized.data.len() > 0);
    assert_eq!(deserialized.prev_page_id, 0);
    assert_eq!(deserialized.next_page_id, 0);

    // Check that all key-value pairs can be retrieved
    for (key, value) in &entries {
        let retrieved_value = deserialized.get(*key).unwrap();
        assert_eq!(retrieved_value, *value);
    }
}

#[test]
fn test_leaf_page_with_linked_pages() {
    // Create a LeafPage with linked pages
    let page_size = 1024;
    let mut leaf_page = LeafPage::new_with_size(page_size);

    // Set prev and next page IDs
    let prev_page_id = 123;
    let next_page_id = 456;
    leaf_page.set_prev_page_id(prev_page_id);
    leaf_page.set_next_page_id(next_page_id);

    // Add a key-value pair
    let key = 3201u64;
    let value = b"test_value";
    assert!(leaf_page.put(key, value));

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not empty
    assert!(!serialized.is_empty());

    // Check that the first byte is the correct page type
    assert_eq!(serialized[0], PageType::LeafPage.to_u8());

    // Deserialize the page
    let deserialized = LeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    // Note: page_size is set to the length of the serialized data, not the original page_size
    assert_eq!(deserialized.metadata.len(), 1);
    assert!(deserialized.data.len() > 0);
    assert_eq!(deserialized.prev_page_id, prev_page_id);
    assert_eq!(deserialized.next_page_id, next_page_id);

    // Check that the key-value pair can be retrieved
    let retrieved_value = deserialized.get(key).unwrap();
    assert_eq!(retrieved_value, value);
}

#[test]
fn test_leaf_page_serialization_format() {
    // Create a LeafPage
    let page_size = 1024;
    let mut leaf_page = LeafPage::new_with_size(page_size);

    // Add a key-value pair
    let key = 3301u64;
    let value = b"test_value";
    assert!(leaf_page.put(key, value));

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check the format of the serialized data
    // First byte is the page type
    assert_eq!(serialized[0], PageType::LeafPage.to_u8());

    // Next 8 bytes are the metadata count (1 in this case)
    let metadata_count = u64::from_le_bytes(serialized[1..9].try_into().unwrap());
    assert_eq!(metadata_count, 1);

    // Next 8 bytes are the data start offset
    let data_start = u64::from_le_bytes(serialized[9..17].try_into().unwrap()) as usize;
    assert!(data_start > 0);

    // Next 8 bytes are the used bytes
    let used_bytes = u64::from_le_bytes(serialized[17..25].try_into().unwrap()) as usize;
    assert_eq!(used_bytes, value.len()); // Only value is stored in data now

    // Next 8 bytes are the prev_page_id
    let prev_page_id = u64::from_le_bytes(serialized[25..33].try_into().unwrap());
    assert_eq!(prev_page_id, 0);

    // Next 8 bytes are the next_page_id
    let next_page_id = u64::from_le_bytes(serialized[33..41].try_into().unwrap());
    assert_eq!(next_page_id, 0);

    // The rest of the data contains the metadata entries and the actual data
    // Let's check that the data contains our key and value
    let data_bytes = &serialized[data_start..];
    assert!(data_bytes.len() >= value.len()); // Only value is stored in data now

    // Only the value should be in the data (key is stored in metadata)
    let mut found_value = false;

    for i in 0..data_bytes.len() - value.len() + 1 {
        if &data_bytes[i..i + value.len()] == value {
            found_value = true;
            break;
        }
    }

    // No need to check for key as it's stored in metadata
    assert!(found_value, "Value not found in serialized data");
}

#[test]
fn test_leaf_page_with_large_data() {
    // Create a LeafPage
    let page_size = 1024;
    let mut leaf_page = LeafPage::new_with_size(page_size);

    // Create a large value (but still small enough to fit in the page)
    let key = 3401u64;
    let value = vec![b'x'; 500]; // 500 bytes of 'x'

    // Insert the large value
    assert!(leaf_page.put(key, &value));

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not empty
    assert!(!serialized.is_empty());

    // Check that the serialized data is not larger than the page size
    assert!(serialized.len() <= page_size);

    // Deserialize the page
    let deserialized = LeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    // Note: page_size is set to the length of the serialized data, not the original page_size
    assert_eq!(deserialized.metadata.len(), 1);
    assert!(deserialized.data.len() >= value.len());

    // Check that the key-value pair can be retrieved
    let retrieved_value = deserialized.get(key).unwrap();
    assert_eq!(retrieved_value, value);
}

#[test]
fn test_leaf_page_with_max_data() {
    // Create a LeafPage
    let page_size = 1024;
    let mut leaf_page = LeafPage::new_with_size(page_size);

    // Calculate how much data we can fit
    // We need to leave room for:
    // - Page type (1 byte)
    // - Metadata count (8 bytes)
    // - Data start offset (8 bytes)
    // - Used bytes (8 bytes)
    // - prev_page_id (8 bytes)
    // - next_page_id (8 bytes)
    // - Metadata entry (16 bytes)
    // Total overhead: 57 bytes
    let overhead = 57;
    let max_data_size = page_size - overhead;

    // Create a key and value that will fill the page
    let key = 3501u64;
    // No need to subtract key length since it's now a u64 stored in metadata
    let value_len = max_data_size;
    let value = vec![b'y'; value_len]; // Fill the rest of the page with 'y'

    // Insert the large value
    assert!(leaf_page.put(key, &value));

    // Serialize the page
    let serialized = leaf_page.serialize();

    // Check that the serialized data is not empty
    assert!(!serialized.is_empty());

    // Check that the serialized data is not larger than the page size
    assert!(serialized.len() <= page_size);

    // Deserialize the page
    let deserialized = LeafPage::deserialize(&serialized);

    // Check that the deserialized page has the expected properties
    // Note: page_size is set to the length of the serialized data, not the original page_size
    assert_eq!(deserialized.metadata.len(), 1);

    // Check that the key-value pair can be retrieved
    let retrieved_value = deserialized.get(key).unwrap();
    assert_eq!(retrieved_value, value);
}
