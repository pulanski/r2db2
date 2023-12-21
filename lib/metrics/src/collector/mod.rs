use crate::metric::Metric;
use async_trait::async_trait;
use std::sync::Arc;
use sysinfo::System;

pub mod cpu;
pub mod memory;

#[async_trait]
pub trait MetricCollector: Send + Sync {
    async fn initialize(&self);
    async fn collect(&self) -> Metric;
    async fn update(&mut self);
    async fn cleanup(&self);
}

/// A reference-counted reference to a [`System`].
/// This is used to collect system metrics like CPU usage,
/// memory usage, etc.
pub type SystemRef = Arc<System>;
