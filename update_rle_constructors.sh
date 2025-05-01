#!/bin/bash
sed -i '' 's/RleLeafPage::new(/RleLeafPage::new_empty(/g' tests/rle_leaf_page_advanced_tests.rs
