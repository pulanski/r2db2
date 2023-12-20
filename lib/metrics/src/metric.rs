use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;

/// Represents different kinds of metrics that can be collected.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MetricKind {
    // System metrics
    CpuUsage,
    MemoryUsage,
    ActiveConnections,

    // Performance metrics
    TransactionRate,
    GarbageCollection,
    QueryExecutionTime,

    // Network metrics
    NetworkIO,

    // Storage metrics
    DiskIO,
    TableSpaceUsage,

    // Efficiency metrics
    CacheHitRate,

    // Replication metrics
    ReplicationDelay,

    // Resource contention metrics
    LockWaitTime,

    // Database operation metrics
    RowOperations,
    IndexUsage,

    // Query type metrics
    SelectQueries,
    InsertQueries,
    UpdateQueries,
    DeleteQueries,
}

/// Represents a metric with its specific data.
/// Each variant corresponds to a different type of metric being tracked.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Metric {
    // System metrics
    CpuUsage(CpuUsage),
    MemoryUsage(MemoryUsage),
    ActiveConnections(ActiveConnections),

    // Performance metrics
    TransactionRate(TransactionRate),
    GarbageCollection(GarbageCollection),
    QueryExecutionTime(QueryExecutionTime),

    // Network metrics
    NetworkIO(NetworkIO),

    // Storage metrics
    DiskIO(DiskIO),
    TableSpaceUsage(TableSpaceUsage),

    // Efficiency metrics
    CacheHitRate(CacheHitRate),

    // Replication metrics
    ReplicationDelay(ReplicationDelay),

    // Resource contention metrics
    LockWaitTime(LockWaitTime),

    // Database operation metrics
    RowOperations(RowOperations),
    IndexUsage(IndexUsage),

    // Query type metrics
    QueryTypeStats(QueryTypeStats),
}

/// Metric for tracking CPU usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuUsage {
    /// CPU usage percentage (0-100%)
    usage_percentage: f32,
}

/// Metric for tracking memory usage.
///
/// Memory usage is measured in megabytes (MB).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryUsage {
    /// Total memory usage in megabytes.
    usage_mb: u64,
}

/// Metric for tracking the number of active connections to the database.
///
/// Active connections are a key indicator of the load on the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveConnections {
    /// Count of currently active connections.
    count: u32,
}

/// Metric for tracking the transaction rate.
///
/// This metric measures the number of transactions processed per second.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRate {
    /// Number of transactions executed per second.
    per_second: f32,
}

/// Metric for tracking garbage collection statistics.
///
/// This includes the total number of garbage collection events and their cumulative duration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GarbageCollection {
    /// Total count of garbage collection events.
    count: u64,
    /// Total duration of all garbage collection events.
    total_duration: Duration,
}

/// Metric for tracking the average execution time of queries.
///
/// This metric is useful for identifying performance issues in query processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutionTime {
    /// Average duration of query execution.
    average_duration: Duration,
}

/// Metric for tracking network I/O.
///
/// This includes the total number of bytes sent and received.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkIO {
    /// Total bytes sent over the network.
    bytes_sent: u64,
    /// Total bytes received over the network.
    bytes_received: u64,
}

/// Metric for tracking disk I/O.
///
/// This metric covers both read and write operations, including the total bytes and time spent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskIO {
    /// Total bytes read from disk.
    read_bytes: u64,
    /// Total bytes written to disk.
    write_bytes: u64,
    /// Total time spent in read operations.
    read_time: Duration,
    /// Total time spent in write operations.
    write_time: Duration,
}

/// Metric for tracking cache hit rate.
///
/// This measures the efficiency of the cache system as a percentage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheHitRate {
    /// Percentage of cache hits out of all cache accesses.
    hit_rate_percentage: f32,
}

/// Metric for tracking replication delay.
///
/// This metric is crucial in distributed database systems to measure lag in data replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationDelay {
    /// Time delay in seconds for data replication.
    delay_seconds: f32,
}

/// Metric for tracking lock wait time.
///
/// Average time spent waiting for locks, indicating contention among transactions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockWaitTime {
    /// Average duration of wait time for locks.
    average_wait_time: Duration,
}

/// Metric for tracking table space usage.
///
/// This includes the amount of space used and available in tablespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSpaceUsage {
    /// Space currently used in tablespace, in megabytes.
    space_used_mb: u64,
    /// Free space available in tablespace, in megabytes.
    space_free_mb: u64,
}

/// Metric for tracking row-level operations.
///
/// Counts the number of read, insert, update, and delete operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowOperations {
    /// Number of row read operations.
    reads: u64,
    /// Number of row insert operations.
    inserts: u64,
    /// Number of row update operations.
    updates: u64,
    /// Number of row delete operations.
    deletes: u64,
}

/// Metric for tracking index usage statistics.
///
/// This includes the number of scans, reads, and writes performed on a specific index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexUsage {
    /// Name of the index.
    index_name: String,
    /// Number of times the index has been scanned.
    scans: u64,
    /// Number of times the index has been read.
    reads: u64,
    /// Number of times the index has been written to.
    writes: u64,
}

/// Metric for tracking statistics of different query types.
///
/// This includes the count of SELECT, INSERT, UPDATE, and DELETE queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTypeStats {
    /// Number of SELECT queries executed.
    select_count: u64,
    /// Number of INSERT queries executed.
    insert_count: u64,
    /// Number of UPDATE queries executed.
    update_count: u64,
    /// Number of DELETE queries executed.
    delete_count: u64,
}

impl Metric {
    pub fn log_metric(&self) {
        match self {
            Metric::CpuUsage(data) => info!("CPU Usage: {}%", data.usage_percentage),
            Metric::MemoryUsage(data) => info!("Memory Usage: {}MB", data.usage_mb),
            Metric::ActiveConnections(data) => info!("Active Connections: {}", data.count),
            Metric::TransactionRate(data) => info!("Transaction Rate: {} tps", data.per_second),
            Metric::GarbageCollection(data) => info!(
                "Garbage Collections: {}, Total Duration: {:?}",
                data.count, data.total_duration
            ),
            Metric::QueryExecutionTime(data) => {
                info!("Average Query Execution Time: {:?}", data.average_duration)
            }
            Metric::NetworkIO(data) => info!(
                "Network I/O - Sent: {} bytes, Received: {} bytes",
                data.bytes_sent, data.bytes_received
            ),
            Metric::DiskIO(data) => info!(
                "Disk I/O - Read: {} bytes, Write: {} bytes, Read Time: {:?}, Write Time: {:?}",
                data.read_bytes, data.write_bytes, data.read_time, data.write_time
            ),
            Metric::CacheHitRate(data) => info!("Cache Hit Rate: {}%", data.hit_rate_percentage),
            Metric::ReplicationDelay(data) => {
                info!("Replication Delay: {} seconds", data.delay_seconds)
            }
            Metric::LockWaitTime(data) => {
                info!("Average Lock Wait Time: {:?}", data.average_wait_time)
            }
            Metric::TableSpaceUsage(data) => info!(
                "Table Space Usage - Used: {}MB, Free: {}MB",
                data.space_used_mb, data.space_free_mb
            ),
            Metric::RowOperations(data) => info!(
                "Row Operations - Reads: {}, Inserts: {}, Updates: {}, Deletes: {}",
                data.reads, data.inserts, data.updates, data.deletes
            ),
            Metric::IndexUsage(data) => info!(
                "Index Usage - Name: {}, Scans: {}, Reads: {}, Writes: {}",
                data.index_name, data.scans, data.reads, data.writes
            ),
            Metric::QueryTypeStats(data) => info!(
                "Query Types - Selects: {}, Inserts: {}, Updates: {}, Deletes: {}",
                data.select_count, data.insert_count, data.update_count, data.delete_count
            ),
        }
    }
}
