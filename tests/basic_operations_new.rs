use data_tree::data_tree::{DataTree2, ResultFormatter, IdentityFormatter};
use data_tree::page_store::{PageStore, InMemoryPageStore};

#[test]
fn test_put_then_get() {
    // Create store with 100 byte pages
    let mut store = InMemoryPageStore::with_page_size(100);
    let page_id = store.allocate_page();
    let formatter = ResultFormatter::new(vec![IdentityFormatter::new(page_id)]);
    let mut tree = DataTree2::new(store, formatter);

    tree.put(1, b"value1").unwrap();
    let actual = tree.get(1).unwrap().unwrap();
    assert_eq!(actual, b"value1");
}