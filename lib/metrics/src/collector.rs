use crate::metric::Metric;
use async_trait::async_trait;

#[async_trait]
pub trait MetricCollector: Send + Sync {
    async fn collect(&self) -> Metric;
}
