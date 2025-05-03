use data_tree::data_tree2::{DataTree2, ResultFormatter, IdentityFormatter};
use data_tree::page_store::{PageStore, InMemoryPageStore};

#[test]
fn test_put_then_get() {
    // Create store with 100 byte pages
    let mut store = InMemoryPageStore::with_page_size(100);
    let page_id = store.allocate_page();
    let formatter = ResultFormatter::new(vec![IdentityFormatter::new(page_id)]);
    let mut tree = DataTree2::new(store, formatter);

    tree.put(1, b"value1").unwrap();
    // At this point there should be 1 page - the one we allocated before creating the tree

    // Verify that we have 1 page in total
    assert_eq!(tree.get_page_count(), 1, "Expected 1 page after put operation");
    let dirty = tree.dirty_pages();
    assert!(dirty.contains(&page_id), "Expected the page to be marked as dirty");
    assert_eq!(dirty.len(), 1, "Expected 1 page to be dirty");

    let actual = tree.get(1).unwrap().unwrap();
    assert_eq!(actual, b"value1");
}