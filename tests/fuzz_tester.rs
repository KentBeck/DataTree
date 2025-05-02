use data_tree::DataTree;
use data_tree::page_store::{PageStore, InMemoryPageStore};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::panic;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use rand::prelude::*;

// Define the operations we'll perform
#[derive(Debug, Clone)]
enum Operation {
    Put { key: u64, value: Vec<u8> },
    Delete { key: u64 },
    Get { key: u64 },
}

// Struct to hold the test state
struct FuzzTest<S: PageStore> {
    tree: DataTree<S>,
    operations: Vec<Operation>,
    expected_data: HashMap<u64, Vec<u8>>,
    rng: StdRng,
}

impl<S: PageStore + 'static> FuzzTest<S> {
    // Create a new fuzz test with the provided page store
    fn new(store: S) -> Self {
        let rng = StdRng::from_entropy();
        let tree = DataTree::new(store);

        FuzzTest {
            tree,
            operations: Vec::new(),
            expected_data: HashMap::new(),
            rng,
        }
    }

    // Generate a random key
    fn random_key(&mut self) -> u64 {
        self.rng.gen()
    }

    // Generate a random value with a random size
    fn random_value(&mut self) -> Vec<u8> {
        let size = self.rng.gen_range(1..=100);
        let mut value = vec![0u8; size];
        self.rng.fill(&mut value[..]);
        value
    }

    // Choose a random existing key, or None if no keys exist
    fn random_existing_key(&mut self) -> Option<u64> {
        if self.expected_data.is_empty() {
            None
        } else {
            let keys: Vec<u64> = self.expected_data.keys().cloned().collect();
            Some(keys[self.rng.gen_range(0..keys.len())])
        }
    }

    // Perform a random operation
    fn random_operation(&mut self) -> Operation {
        // Decide which operation to perform
        let op_type = self.rng.gen_range(0..=2);

        match op_type {
            0 => {
                // Put operation
                let key = self.random_key();
                let value = self.random_value();
                Operation::Put {
                    key,
                    value
                }
            },
            1 => {
                // Delete operation
                if let Some(key) = self.random_existing_key() {
                    Operation::Delete { key }
                } else {
                    // If no keys exist, do a put instead
                    let key = self.random_key();
                    let value = self.random_value();
                    Operation::Put {
                        key,
                        value
                    }
                }
            },
            2 => {
                // Get operation
                if let Some(key) = self.random_existing_key() {
                    Operation::Get { key }
                } else {
                    // If no keys exist, do a put instead
                    let key = self.random_key();
                    let value = self.random_value();
                    Operation::Put {
                        key,
                        value
                    }
                }
            },
            _ => unreachable!(),
        }
    }

    // Execute an operation and update the expected state
    fn execute_operation(&mut self, op: &Operation) -> Result<(), String> {
        match op {
            Operation::Put { key, value } => {
                match self.tree.put(*key, value) {
                    Ok(_) => {
                        // Update expected state
                        self.expected_data.insert(*key, value.clone());
                        Ok(())
                    },
                    Err(e) => Err(format!("Put operation failed: {}", e)),
                }
            },
            Operation::Delete { key } => {
                match self.tree.delete(*key) {
                    Ok(_) => {
                        // Update expected state
                        self.expected_data.remove(key);
                        Ok(())
                    },
                    Err(e) => Err(format!("Delete operation failed: {}", e)),
                }
            },
            Operation::Get { key } => {
                match self.tree.get(*key) {
                    Ok(result) => {
                        // Verify the result matches our expected state
                        let expected = self.expected_data.get(key);
                        match (result, expected) {
                            (Some(actual), Some(expected)) => {
                                if actual == *expected {
                                    Ok(())
                                } else {
                                    Err(format!("Get returned incorrect value for key {}: expected {:?}, got {:?}", key, expected, actual))
                                }
                            },
                            (None, None) => Ok(()),
                            (Some(actual), None) => {
                                Err(format!("Get returned value {:?} for key {} but expected None", actual, key))
                            },
                            (None, Some(expected)) => {
                                Err(format!("Get returned None for key {} but expected {:?}", key, expected))
                            },
                        }
                    },
                    Err(e) => Err(format!("Get operation failed: {}", e)),
                }
            },
        }
    }

    // Run the fuzz test for a specified duration
    fn run(&mut self, duration: Duration) -> Result<(), String> {
        let start_time = Instant::now();
        let mut operation_count = 0;

        while start_time.elapsed() < duration {
            let op = self.random_operation();
            self.operations.push(op.clone());

            if let Err(e) = self.execute_operation(&op) {
                return Err(e);
            }

            operation_count += 1;

            // Print progress every 1000 operations
            if operation_count % 1000 == 0 {
                println!("Executed {} operations in {:?}", operation_count, start_time.elapsed());
            }
        }

        println!("Completed {} operations in {:?}", operation_count, start_time.elapsed());

        // Verify all keys in our expected state
        for (key, expected_value) in &self.expected_data {
            match self.tree.get(*key) {
                Ok(Some(actual_value)) => {
                    if actual_value != *expected_value {
                        return Err(format!(
                            "Final verification failed: key {} has value {:?} but expected {:?}",
                            key, actual_value, expected_value
                        ));
                    }
                },
                Ok(None) => {
                    return Err(format!(
                        "Final verification failed: key {} not found but expected {:?}",
                        key, expected_value
                    ));
                },
                Err(e) => {
                    return Err(format!(
                        "Final verification failed: error getting key {}: {}",
                        key, e
                    ));
                },
            }
        }

        Ok(())
    }

    // Save the sequence of operations to a file for replay
    fn save_operations(&self, filename: &str) -> std::io::Result<()> {
        let mut file = File::create(filename)?;

        writeln!(file, "# DataTree Fuzz Test Operations")?;
        writeln!(file, "# Page size: {}", self.tree.store().page_size())?;
        writeln!(file, "# Number of operations: {}", self.operations.len())?;
        writeln!(file, "# Timestamp: {}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())?;
        writeln!(file, "")?;

        for (i, op) in self.operations.iter().enumerate() {
            match op {
                Operation::Put { key, value } => {
                    writeln!(file, "Operation {}: Put key={} value_size={}", i, key, value.len())?;
                    writeln!(file, "  Value: {:?}", value)?;
                },
                Operation::Delete { key } => {
                    writeln!(file, "Operation {}: Delete key={}", i, key)?;
                },
                Operation::Get { key } => {
                    writeln!(file, "Operation {}: Get key={}", i, key)?;
                },
            }
        }

        Ok(())
    }
}

// A custom PageStore implementation that wraps InMemoryPageStore
struct CustomPageStore {
    inner: InMemoryPageStore,
}

impl Clone for CustomPageStore {
    fn clone(&self) -> Self {
        // Create a new InMemoryPageStore with the same page size
        let page_size = self.inner.page_size();
        Self {
            inner: InMemoryPageStore::with_page_size(page_size),
        }
    }
}

impl CustomPageStore {
    fn new(page_size: usize) -> Self {
        Self {
            inner: InMemoryPageStore::with_page_size(page_size),
        }
    }
}

impl PageStore for CustomPageStore {
    fn get_page_bytes(&self, page_id: u64) -> Result<Vec<u8>, Box<dyn Error>> {
        self.inner.get_page_bytes(page_id)
    }

    fn put_page_bytes(&mut self, page_id: u64, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        self.inner.put_page_bytes(page_id, bytes)
    }

    fn allocate_page(&mut self) -> u64 {
        self.inner.allocate_page()
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.inner.flush()
    }

    fn page_size(&self) -> usize {
        self.inner.page_size()
    }

    fn get_next_page_id(&self, page_id: u64) -> Option<u64> {
        self.inner.get_next_page_id(page_id)
    }

    fn get_prev_page_id(&self, page_id: u64) -> Option<u64> {
        self.inner.get_prev_page_id(page_id)
    }

    fn link_pages(&mut self, prev_page_id: u64, next_page_id: u64) -> Result<(), Box<dyn Error>> {
        self.inner.link_pages(prev_page_id, next_page_id)
    }

    fn page_exists(&self, page_id: u64) -> bool {
        self.inner.page_exists(page_id)
    }

    fn free_page(&mut self, page_id: u64) -> Result<(), Box<dyn Error>> {
        self.inner.free_page(page_id)
    }

    fn get_page_count(&self) -> usize {
        self.inner.get_page_count()
    }
}

// Helper function to run a fuzz test with any PageStore implementation
#[allow(dead_code)]
fn run_fuzz_test_with_store<S: PageStore + Clone + 'static>(store: S, duration: Duration) -> Result<(), String> {
    let mut fuzz_test = FuzzTest::new(store);
    fuzz_test.run(duration)
}

#[test]
fn test_fuzz_data_tree_with_in_memory_store() {
    // Set up panic hook to save operations on failure
    let old_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        if let Some(fuzz_test) = CURRENT_FUZZ_TEST.with(|cell| cell.borrow().clone()) {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let filename = format!("fuzz_failure_{}.log", timestamp);
            if let Err(e) = fuzz_test.save_operations(&filename) {
                eprintln!("Failed to save operations: {}", e);
            } else {
                eprintln!("Saved failing operations to {}", filename);
            }
        }

        // Call the original panic hook
        old_hook(panic_info);
    }));

    // Create a random page size between 1024 and 4096 bytes
    let mut rng = StdRng::from_entropy();
    let page_size = rng.gen_range(1024..=4096);

    // Create an InMemoryPageStore with the random page size
    let store = InMemoryPageStore::with_page_size(page_size);

    // Create and run the fuzz test
    let mut fuzz_test = FuzzTest::new(store);

    // Store the fuzz test in thread local storage for the panic hook
    CURRENT_FUZZ_TEST.with(|cell| {
        *cell.borrow_mut() = Some(fuzz_test.clone());
    });

    // Parse duration from environment variable or use default (10 seconds)
    let duration_str = std::env::var("FUZZ_DURATION").unwrap_or_else(|_| "10s".to_string());
    let duration = parse_duration(&duration_str).unwrap_or_else(|_| {
        println!("Invalid duration format: {}, using default of 10 seconds", duration_str);
        Duration::from_secs(10)
    });

    println!("Running fuzz test for {:?}", duration);

    // Run the fuzz test for the specified duration
    if let Err(e) = fuzz_test.run(duration) {
        // Save operations on error
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let filename = format!("fuzz_failure_{}.log", timestamp);
        if let Err(save_err) = fuzz_test.save_operations(&filename) {
            eprintln!("Failed to save operations: {}", save_err);
        } else {
            eprintln!("Saved failing operations to {}", filename);
        }

        panic!("Fuzz test failed: {}", e);
    }

    // Clean up
    CURRENT_FUZZ_TEST.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

#[test]
fn test_fuzz_data_tree_with_custom_store() {
    // Set up panic hook to save operations on failure
    let old_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        if let Some(fuzz_test) = CURRENT_CUSTOM_FUZZ_TEST.with(|cell| cell.borrow().clone()) {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let filename = format!("fuzz_failure_custom_{}.log", timestamp);
            if let Err(e) = fuzz_test.save_operations(&filename) {
                eprintln!("Failed to save operations: {}", e);
            } else {
                eprintln!("Saved failing operations to {}", filename);
            }
        }

        // Call the original panic hook
        old_hook(panic_info);
    }));

    // Create a random page size between 1024 and 4096 bytes
    let mut rng = StdRng::from_entropy();
    let page_size = rng.gen_range(1024..=4096);

    // Create a CustomPageStore with the random page size
    let store = CustomPageStore::new(page_size);

    // Create and run the fuzz test
    let mut fuzz_test = FuzzTest::new(store);

    // Store the fuzz test in thread local storage for the panic hook
    CURRENT_CUSTOM_FUZZ_TEST.with(|cell| {
        *cell.borrow_mut() = Some(fuzz_test.clone());
    });

    // Parse duration from environment variable or use default (10 seconds)
    let duration_str = std::env::var("FUZZ_DURATION").unwrap_or_else(|_| "1s".to_string());
    let duration = parse_duration(&duration_str).unwrap_or_else(|_| {
        println!("Invalid duration format: {}, using default of 1 second", duration_str);
        Duration::from_secs(1)
    });

    println!("Running custom store fuzz test for {:?}", duration);

    // Run the fuzz test for the specified duration
    if let Err(e) = fuzz_test.run(duration) {
        // Save operations on error
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let filename = format!("fuzz_failure_custom_{}.log", timestamp);
        if let Err(save_err) = fuzz_test.save_operations(&filename) {
            eprintln!("Failed to save operations: {}", save_err);
        } else {
            eprintln!("Saved failing operations to {}", filename);
        }

        panic!("Fuzz test failed: {}", e);
    }

    // Clean up
    CURRENT_CUSTOM_FUZZ_TEST.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

// Thread local storage for the current fuzz test
thread_local! {
    static CURRENT_FUZZ_TEST: std::cell::RefCell<Option<FuzzTest<InMemoryPageStore>>> = std::cell::RefCell::new(None);
    static CURRENT_CUSTOM_FUZZ_TEST: std::cell::RefCell<Option<FuzzTest<CustomPageStore>>> = std::cell::RefCell::new(None);
}

// Parse a duration string like "30s" or "20m"
fn parse_duration(duration_str: &str) -> Result<Duration, String> {
    let duration_str = duration_str.trim().to_lowercase();

    // Check if the string is empty
    if duration_str.is_empty() {
        return Err("Empty duration string".to_string());
    }

    // Find the last character (should be the unit)
    let last_char = duration_str.chars().last().unwrap();

    // Parse the numeric part
    let numeric_part = &duration_str[0..duration_str.len() - 1];
    let value = match numeric_part.parse::<u64>() {
        Ok(v) => v,
        Err(_) => return Err(format!("Invalid numeric value: {}", numeric_part)),
    };

    // Parse the unit
    match last_char {
        's' => Ok(Duration::from_secs(value)),
        'm' => Ok(Duration::from_secs(value * 60)),
        'h' => Ok(Duration::from_secs(value * 3600)),
        _ => Err(format!("Unknown time unit: {}", last_char)),
    }
}

// Implement Clone for FuzzTest with InMemoryPageStore
impl Clone for FuzzTest<InMemoryPageStore> {
    fn clone(&self) -> Self {
        // Create a new DataTree with the same page size
        let page_size = self.tree.store().page_size();
        let store = InMemoryPageStore::with_page_size(page_size);
        let tree = DataTree::new(store);

        // Create a new FuzzTest with the same operations and expected data
        FuzzTest {
            tree,
            operations: self.operations.clone(),
            expected_data: self.expected_data.clone(),
            rng: StdRng::from_entropy(), // Create a new RNG
        }
    }
}

// Implement Clone for FuzzTest with CustomPageStore
impl Clone for FuzzTest<CustomPageStore> {
    fn clone(&self) -> Self {
        // Create a new DataTree with the same page size
        let page_size = self.tree.store().page_size();
        let store = CustomPageStore::new(page_size);
        let tree = DataTree::new(store);

        // Create a new FuzzTest with the same operations and expected data
        FuzzTest {
            tree,
            operations: self.operations.clone(),
            expected_data: self.expected_data.clone(),
            rng: StdRng::from_entropy(), // Create a new RNG
        }
    }
}
