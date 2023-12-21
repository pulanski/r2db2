use crate::{
    collector::MetricCollector,
    metric::{Metric, MetricKind},
};
use core::fmt;
use dashmap::DashMap;
use serde_json::json;
use std::sync::Arc;

/// A reference-counted reference to a [`MetricsManager`].
pub type MetricsManagerRef = Arc<MetricsManager>;

pub struct MetricsManager {
    metrics: Arc<DashMap<MetricKind, Metric>>,
    collectors: Vec<Box<dyn MetricCollector>>,
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
        self.collectors.push(Box::new(collector));
    }

    pub async fn collect_metrics(&self) -> Vec<Metric> {
        let mut metrics = vec![];

        for collector in self.collectors.iter() {
            metrics.push(collector.collect().await);
        }

        metrics
    }
}
