use data_tree::DataTree;

use data_tree::data_tree::PageType;
use data_tree::branch_page::BranchPage;
use data_tree::page_store::{PageStore, InMemoryPageStore};

#[test]
fn test_branch_root_creation() {
    // Create a DataTree with a BranchPage as the root
    let store = InMemoryPageStore::with_page_size(1024);
    let tree = DataTree::new(store);

    // Get the root page
    let root_page_bytes = tree.store().get_page_bytes(tree.root_page_id()).unwrap();

    // Check that the root page is a BranchPage
    let page_type = PageType::from_u8(root_page_bytes[0]).unwrap();
    assert_eq!(page_type, PageType::BranchPage);

    // Deserialize the BranchPage
    let branch_page = BranchPage::deserialize(&root_page_bytes);

    // Check that the BranchPage has one entry
    assert_eq!(branch_page.entries().len(), 1);

    // Check that the entry points to a LeafPage
    let leaf_page_id = branch_page.entries()[0].page_id;
    let leaf_page_bytes = tree.store().get_page_bytes(leaf_page_id).unwrap();
    let leaf_page_type = PageType::from_u8(leaf_page_bytes[0]).unwrap();
    assert_eq!(leaf_page_type, PageType::LeafPage);
}

#[test]
fn test_branch_root_operations() {
    // Create a DataTree with a BranchPage as the root
    let store = InMemoryPageStore::with_page_size(1024);
    let mut tree = DataTree::new(store);

    // Insert some key-value pairs
    for i in 0..10 {
        let key = 1900 + i as u64;
        let value = format!("value{}", i).into_bytes();
        tree.put(key, &value).unwrap();
    }

    // Retrieve the values
    for i in 0..10 {
        let key = 1900 + i as u64;
        let value = tree.get(key).unwrap().unwrap();
        assert_eq!(value, format!("value{}", i).into_bytes());
    }

    // Delete some key-value pairs
    for i in 0..5 {
        let key = 1900 + i as u64;
        assert!(tree.delete(key).unwrap());
    }

    // Check that the deleted keys are gone
    for i in 0..5 {
        let key = 1900 + i as u64;
        assert!(tree.get(key).unwrap().is_none());
    }

    // Check that the remaining keys are still there
    for i in 5..10 {
        let key = 1900 + i as u64;
        let value = tree.get(key).unwrap().unwrap();
        assert_eq!(value, format!("value{}", i).into_bytes());
    }
}
