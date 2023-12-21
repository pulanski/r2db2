use crate::{
    collector::MetricCollector,
    metric::{Metric, MetricKind},
};
use core::fmt;
use dashmap::DashMap;
use serde_json::json;
use std::sync::Arc;
use thiserror::Error;
use tracing::debug;

#[derive(Error, Debug)]
pub enum MetricsServerError {
    #[error("Port allocation failed after trying {0} ports")]
    PortAllocationError(u16),
}

/// A reference-counted reference to a [`MetricsManager`].
pub type MetricsManagerRef = Arc<MetricsManager>;

/// A reference-counted reference to a [`MetricCollector`].
pub type MetricCollectorRef = Arc<dyn MetricCollector>;

pub struct MetricsManager {
    metrics: Arc<DashMap<MetricKind, Metric>>,
    collectors: Vec<MetricCollectorRef>,
}

impl fmt::Debug for MetricsManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsManager")
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl MetricsManager {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(DashMap::new()),
            collectors: vec![],
        }
    }

    pub async fn update_metric(&self, metric_type: MetricKind, metric: Metric) {
        self.metrics.insert(metric_type, metric);
    }

    pub async fn get_metrics(&self) -> String {
        let mut all_metrics = serde_json::Map::new();

        for entry in self.metrics.iter() {
            all_metrics.insert(
                format!("{:?}", entry.key()),
                serde_json::to_value(entry.value()).unwrap(),
            );
        }

        json!(all_metrics).to_string()
    }

    pub fn register_collector(&mut self, collector: impl MetricCollector + 'static) {
        self.collectors.push(Arc::new(collector));
    }

    pub async fn collect_metrics(&self) -> Vec<Metric> {
        let mut collected_metrics = Vec::new();

        for collector in &self.collectors {
            debug!("Collecting metrics from {}", collector.name());
            let metric = collector.collect().await;
            self.metrics.insert(metric.kind(), metric.clone()); // Assuming Metric has a kind() method
            collected_metrics.push(metric);
        }

        collected_metrics
    }
}
