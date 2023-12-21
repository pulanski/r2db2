use super::{MetricCollector, SystemRef};
use crate::metric::{CpuUsage, Metric};
use async_trait::async_trait;
use getset::{Getters, Setters};
use std::sync::Arc;
use sysinfo::{CpuRefreshKind, RefreshKind, System};
use tracing::{instrument, trace};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, Getters, Setters, TypedBuilder)]
pub struct CpuUsageCollector {
    system: SystemRef,
}

impl CpuUsageCollector {
    pub fn new() -> Self {
        let system =
            System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::everything()));

        CpuUsageCollector::builder()
            .system(Arc::new(system))
            .build()
    }
}

#[async_trait]
impl MetricCollector for CpuUsageCollector {
    #[inline]
    #[instrument(skip(self))]
    async fn initialize(&self) {
        trace!("Initializing CPU usage collector (no-op)")
    }

    #[inline]
    #[instrument(skip(self))]
    async fn collect(&self) -> Metric {
        let cpu_usage = self.system.global_cpu_info().cpu_usage();
        Metric::CpuUsage(CpuUsage::builder().usage_percentage(cpu_usage).build())
    }

    #[inline]
    #[instrument(skip(self))]
    async fn update(&mut self) {
        // Asynchronously refresh the system information
        let mut s =
            System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::everything()));

        // Wait a bit because CPU usage is based on diff.
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        // Refresh CPUs again.
        s.refresh_cpu();

        self.system = Arc::new(s);
    }

    #[inline]
    #[instrument(skip(self))]
    async fn cleanup(&self) {
        trace!("Cleaning up CPU usage collector (no-op)")
    }
}
