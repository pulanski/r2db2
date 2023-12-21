use crate::metric::{Metric, MetricKind};
use dashmap::DashMap;
use serde_json::json;
use std::sync::Arc;

/// A reference-counted reference to a [`MetricsManager`].
pub type MetricsManagerRef = Arc<MetricsManager>;

#[derive(Debug, Clone)]
pub struct MetricsManager {
    metrics: Arc<DashMap<MetricKind, Metric>>,
}

impl MetricsManager {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(DashMap::new()),
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
}
