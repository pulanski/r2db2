mod manager;
mod scheduler;

use std::sync::Arc;

pub use manager::DiskManager;
use tempfile::TempDir;

fn setup() -> (Arc<DiskManager>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_name = format!("test_{}.db", rand::random::<u32>());
    let db_path = temp_dir.path().join(db_name).to_string_lossy().to_string();
    let dm = Arc::new(DiskManager::new(&db_path).expect("Failed to create disk manager"));

    (dm, temp_dir)
}
