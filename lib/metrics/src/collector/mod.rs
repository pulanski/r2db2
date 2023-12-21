use crate::metric::Metric;
use async_trait::async_trait;
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::Mutex;

pub mod cpu;
pub mod memory;

#[async_trait]
pub trait MetricCollector: Send + Sync {
    fn name(&self) -> String;
    async fn collect(&self) -> Metric;
}

/// A reference-counted reference to a [`System`].
/// This is used to collect system metrics like CPU usage,
/// memory usage, etc.
pub type SystemRef = Arc<Mutex<System>>;
