#[cfg(test)]
mod tests {
    use crate::leaf_page::BranchPage;
    use crate::PageType;

    #[test]
    fn test_branch_page_operations() {
        // Create a branch page
        let mut branch_page = BranchPage::new(100);

        // Insert some entries
        assert!(branch_page.insert(1, 10)); // Page 1 starts with key 10
        assert!(branch_page.insert(2, 20)); // Page 2 starts with key 20
        assert!(branch_page.insert(3, 30)); // Page 3 starts with key 30

        // Test finding page IDs
        assert_eq!(branch_page.find_page_id(5), Some(1)); // Before first key
        assert_eq!(branch_page.find_page_id(10), Some(1)); // First key
        assert_eq!(branch_page.find_page_id(15), Some(1)); // Between 10 and 20
        assert_eq!(branch_page.find_page_id(20), Some(2)); // Second key
        assert_eq!(branch_page.find_page_id(25), Some(2)); // Between 20 and 30
        assert_eq!(branch_page.find_page_id(30), Some(3)); // Last key
        assert_eq!(branch_page.find_page_id(35), Some(3)); // After last key

        // Test serialization and deserialization
        let serialized = branch_page.serialize();
        let deserialized = BranchPage::deserialize(&serialized);

        // Verify page type
        assert_eq!(deserialized.page_type(), PageType::BranchPage);

        // Verify entries through find_page_id
        assert_eq!(deserialized.find_page_id(10), Some(1));
        assert_eq!(deserialized.find_page_id(20), Some(2));
        assert_eq!(deserialized.find_page_id(30), Some(3));
    }

    #[test]
    fn test_branch_page_linking() {
        let mut branch_page = BranchPage::new(100);

        // Test page linking
        branch_page.set_prev_page_id(42);
        branch_page.set_next_page_id(43);

        assert_eq!(branch_page.prev_page_id(), 42);
        assert_eq!(branch_page.next_page_id(), 43);

        // Verify links are preserved in serialization
        let serialized = branch_page.serialize();
        let deserialized = BranchPage::deserialize(&serialized);

        assert_eq!(deserialized.prev_page_id(), 42);
        assert_eq!(deserialized.next_page_id(), 43);
    }
} 