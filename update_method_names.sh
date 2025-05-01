#!/bin/bash
sed -i '' 's/leaf_page\.insert(/leaf_page\.put(/g' tests/rle_leaf_page_advanced_tests.rs
