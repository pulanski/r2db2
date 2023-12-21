use super::{MetricCollector, SystemRef};
use crate::metric::{MemoryUsage, Metric};
use async_trait::async_trait;
use getset::{Getters, Setters};
use std::sync::Arc;
use sysinfo::System;
use tracing::{instrument, trace};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, Getters, Setters, TypedBuilder)]
#[getset(get = "pub")]
pub struct MemoryUsageCollector {
    system: SystemRef,
}

#[async_trait]
impl MetricCollector for MemoryUsageCollector {
    #[inline]
    #[instrument(skip(self))]
    async fn initialize(&self) {
        trace!("Initializing memory usage collector (no-op)")
    }

    #[inline]
    #[instrument(skip(self))]
    async fn collect(&self) -> Metric {
        let memory_usage = self.system.used_memory();
        // let total_memory = self.system.total_memory(); // TODO: Add this to the metric
        Metric::MemoryUsage(MemoryUsage::builder().usage_mb(memory_usage / 1024).build())
    }

    #[inline]
    #[instrument(skip(self))]
    async fn update(&mut self) {
        // Asynchronously refresh the memory information
        // self.system.refresh_memory();
        let mut s = System::new();

        // Refresh Memory again.
        s.refresh_memory();

        self.system = Arc::new(s);
    }

    #[inline]
    #[instrument(skip(self))]
    async fn cleanup(&self) {
        trace!("Cleaning up memory usage collector (no-op)")
    }
}
