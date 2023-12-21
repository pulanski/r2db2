use super::{MetricCollector, SystemRef};
use crate::metric::{MemoryUsage, Metric};
use async_trait::async_trait;
use getset::{Getters, Setters};
use tracing::trace;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, Getters, Setters, TypedBuilder)]
#[getset(get = "pub")]
pub struct MemoryUsageCollector {
    system: SystemRef,
}

#[async_trait]
impl MetricCollector for MemoryUsageCollector {
    #[inline]
    fn name(&self) -> String {
        "Memory Usage Collector".to_string()
    }

    #[inline]
    async fn collect(&self) -> Metric {
        let mut system = self.system.lock().await;

        let prev = system.used_memory();
        system.refresh_memory();
        let current = system.used_memory();

        trace!("{}", format_memory_usage_diff(prev, current));
        Metric::MemoryUsage(MemoryUsage::builder().usage_mb(current).build())
    }
}
/// Formats memory size in the most appropriate unit (GB or MB).
fn format_memory_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    match bytes {
        bytes if bytes >= GB => format!("{:.2} GB", bytes as f64 / GB as f64),
        _ => format!("{:.2} MB", bytes as f64 / MB as f64),
    }
}

/// Calculates and formats the absolute difference and percentage change between two memory sizes.
fn format_memory_diff(prev: u64, current: u64) -> String {
    let diff = (current as i64 - prev as i64).abs() as u64;
    let diff_pct = if prev != 0 {
        (diff as f64 / prev as f64) * 100.0
    } else {
        0.0
    };

    let sign = if current > prev { "+" } else { "-" };
    format!("{}{} ({:.2}%)", sign, format_memory_size(diff), diff_pct)
}

/// Pretty-print the memory usage change with GB or MB units depending on the size.
pub fn format_memory_usage_diff(prev: u64, current: u64) -> String {
    let prev_str = format_memory_size(prev);
    let current_str = format_memory_size(current);
    let diff_str = format_memory_diff(prev, current);

    let change_type = if current > prev {
        "increased"
    } else {
        "decreased"
    };

    format!(
        "Memory usage {} from {} to {} (change: {})",
        change_type, prev_str, current_str, diff_str
    )
}

/// Pretty-print the memory usage change with GB or MB units depending on the size.
pub fn format_memory_usage(usage: u64) -> String {
    format!("Memory usage: {}", format_memory_size(usage))
}
