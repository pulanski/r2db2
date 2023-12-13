mod manager;
mod scheduler;

pub use manager::DiskManager;
pub use scheduler::*;

use std::sync::Arc;
use tempfile::TempDir;

pub fn setup_dm() -> (Arc<DiskManager>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_name = format!("test_{}.db", rand::random::<u32>());
    let db_path = temp_dir.path().join(db_name).to_string_lossy().to_string();
    let dm = Arc::new(DiskManager::new(&db_path).expect("Failed to create disk manager"));

    (dm, temp_dir)
}
