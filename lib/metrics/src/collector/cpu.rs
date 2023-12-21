use super::{MetricCollector, SystemRef};
use crate::metric::{CpuUsage, Metric};
use async_trait::async_trait;
use getset::{Getters, Setters};
use std::sync::Arc;
use sysinfo::{CpuRefreshKind, RefreshKind, System};
use tokio::sync::Mutex;
use tracing::trace;
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
            .system(Arc::new(Mutex::new(system)))
            .build()
    }
}

#[async_trait]
impl MetricCollector for CpuUsageCollector {
    #[inline]
    fn name(&self) -> String {
        "CPU Usage Collector".to_string()
    }

    #[inline]
    async fn collect(&self) -> Metric {
        let mut system = self.system.lock().await;

        let prev = system.global_cpu_info().cpu_usage();
        system.refresh_cpu();
        let current = system.global_cpu_info().cpu_usage();

        trace!("{}", format_cpu_usage_diff(prev, current));
        Metric::CpuUsage(CpuUsage::builder().usage_percentage(current).build())
    }
}

/// Formats the CPU usage change.
fn format_cpu_usage_diff(prev: f32, current: f32) -> String {
    let change_type = if current > prev {
        "increased"
    } else {
        "decreased"
    };

    let diff = (current - prev).abs();
    let prev = format_cpu_pct(prev);
    let current = format_cpu_pct(current);

    format!(
        "CPU usage {} from {:.2}% to {:.2}% (change: {:.2}%)",
        change_type, prev, current, diff
    )
}

/// Pretty-print the CPU percentage.
pub fn format_cpu_pct(usage: f32) -> String {
    format!("{:.2}%", usage)
}

/// Pretty-print the CPU usage.
pub fn format_cpu_usage(usage: f32) -> String {
    format!("CPU usage: {}", format_cpu_pct(usage))
}
