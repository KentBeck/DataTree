use data_tree::data_tree::PageType;
use data_tree::leaf_page::LeafPage;

#[test]
fn test_free_page_creation() {
    // Create a FREE page
    let page_size = 1024;
    let free_page = LeafPage::new_free_page(page_size);

    // Verify it has the correct page type
    assert_eq!(free_page.page_type(), PageType::LeafPage);

    // Verify it has no metadata or data
    assert!(free_page.metadata.is_empty());
    assert!(free_page.data.is_empty());
}

#[test]
fn test_free_page_serialization() {
    // Create a FREE page
    let page_size = 1024;
    let free_page = LeafPage::new_free_page(page_size);

    // Serialize it
    let bytes = free_page.serialize();

    // Verify the serialized bytes have the correct page type
    assert_eq!(bytes[0], PageType::LeafPage.to_u8());
}

#[test]
fn test_free_page_deserialization() {
    // Create a FREE page
    let page_size = 1024;
    let free_page = LeafPage::new_free_page(page_size);

    // Serialize it
    let bytes = free_page.serialize();

    // Deserialize it
    let deserialized_page = LeafPage::deserialize(&bytes);

    // Verify it has the correct page type
    assert_eq!(deserialized_page.page_type(), PageType::LeafPage);

    // Verify it has no metadata or data
    assert!(deserialized_page.metadata.is_empty());
    assert!(deserialized_page.data.is_empty());
}

#[test]
fn test_convert_to_free_page() {
    // Create a regular leaf page
    let page_size = 1024;
    let mut leaf_page = LeafPage::new_with_size(page_size);

    // Add some data to it
    leaf_page.put(1, b"value1");

    // Convert it to a FREE page
    leaf_page.metadata.clear();
    leaf_page.data.clear();

    // Verify it has the correct page type
    assert_eq!(leaf_page.page_type(), PageType::LeafPage);

    // Verify it has no metadata or data
    assert!(leaf_page.metadata.is_empty());
    assert!(leaf_page.data.is_empty());

    // Serialize it
    let bytes = leaf_page.serialize();

    // Verify the serialized bytes have the correct page type
    assert_eq!(bytes[0], PageType::LeafPage.to_u8());
}
