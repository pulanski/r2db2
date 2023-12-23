#![allow(dead_code)]

use super::DiskManager;
#[allow(unused_imports)]
use crate::disk::setup_dm;
use anyhow::Result;
use common::{PageId, PAGE_SIZE};
use getset::{Getters, Setters};
use parking_lot::Mutex;
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tracing::{debug, error, info, instrument, trace, warn};
use typed_builder::TypedBuilder;

#[derive(Error, Debug)]
pub enum DiskSchedulerError {
    #[error("Failed to schedule the disk operation")]
    ScheduleError(#[from] mpsc::error::SendError<DiskRequest>),

    #[error("Failed to complete the disk operation")]
    CompletionError(#[from] oneshot::error::RecvError),

    #[error("Disk Manager Error: {0}")]
    DiskManagerError(String),
    // ... TODO: future other error types ...
}

/// Represents a disk operation request with a completion signal.
#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub")]
pub struct DiskRequest {
    /// Whether this is a read or write request
    is_write: bool,
    /// The data to be written, or the buffer to be filled with data
    data: Vec<u8>,
    /// The page to be read or written
    page_id: u32,
    /// The callback to be invoked when the request is complete
    /// (i.e. the data has been written or read)
    completion_signal: Option<oneshot::Sender<()>>,
    /// Channel to send back read data
    read_data_sender: Option<mpsc::Sender<Vec<u8>>>,
    /// The priority of the request
    priority: u8, // Lower number means higher priority
}

impl Clone for DiskRequest {
    fn clone(&self) -> Self {
        DiskRequest::builder()
            .is_write(self.is_write)
            .data(self.data.clone())
            .page_id(self.page_id)
            .completion_signal(None) // NOTE: Reset the completion signal (if any)
            .read_data_sender(self.read_data_sender.clone())
            .priority(self.priority)
            .build()
    }
}

impl DiskRequest {
    pub fn new(
        is_write: bool,
        data: Vec<u8>,
        page_id: u32,
        completion_signal: Option<oneshot::Sender<()>>,
        read_data_sender: Option<mpsc::Sender<Vec<u8>>>,
        priority: u8,
    ) -> Self {
        DiskRequest::builder()
            .is_write(is_write)
            .data(data)
            .page_id(page_id)
            .completion_signal(completion_signal)
            .read_data_sender(read_data_sender)
            .priority(priority)
            .build()
    }

    pub async fn complete(&mut self) {
        if let Some(sender) = self.completion_signal.take() {
            let _ = sender.send(()); // Ignoring the result as receiver may be dropped
        }
    }
}

impl Ord for DiskRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority.cmp(&self.priority)
    }
}

impl PartialOrd for DiskRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DiskRequest {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for DiskRequest {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_disk_request_completion_signal() {
        let (tx, rx) = oneshot::channel();
        let mut request = DiskRequest::new(false, vec![1, 2, 3, 4], 1, Some(tx), None, 0);

        request.complete().await;

        assert!(rx.await.is_ok(), "Completion signal should have been sent");
    }
}

pub enum WriteStrategy {
    Immediate,
    Buffered,
}

#[derive(Debug)]
pub struct DiskScheduler {
    disk_manager: Arc<DiskManager>,
    sender: mpsc::Sender<DiskRequest>,
    write_buffer: Arc<Mutex<Vec<DiskRequest>>>,
    last_flush: Mutex<Instant>,
    flush_interval: Duration,
}

impl DiskScheduler {
    /// The maximum number of pending requests that can be queued up
    /// before the scheduler blocks the caller.
    const MAX_PENDING_REQUESTS: usize = 32;

    /// The interval at which the write buffer is flushed to disk.
    const FLUSH_INTERVAL: u64 = 5; // seconds

    /// The maximum number of write requests that can be buffered
    /// before the write buffer is flushed to disk.
    const MAX_BUFFER_SIZE: usize = 32;

    #[instrument(skip(disk_manager))]
    pub fn new(disk_manager: Arc<DiskManager>) -> Arc<Self> {
        let (sender, mut receiver) = mpsc::channel::<DiskRequest>(Self::MAX_PENDING_REQUESTS);
        let disk_manager_clone = disk_manager.clone();

        debug!("Spawning DiskScheduler worker task");

        tokio::spawn(async move {
            trace!("DiskScheduler worker started");
            while let Some(mut request) = receiver.recv().await {
                trace!(
                    page_id = request.page_id,
                    is_write = request.is_write,
                    "Processing disk request"
                );
                let disk_manager_clone = disk_manager_clone.clone();
                task::spawn(async move {
                    if request.is_write {
                        trace!(page_id = request.page_id, "Writing to disk");
                        if let Err(e) = disk_manager_clone
                            .write_page_async(request.page_id, &request.data)
                            .await
                        {
                            error!(error = %e, "Failed to write to disk");
                        }
                    } else {
                        trace!(page_id = request.page_id, "Reading from disk");
                        let mut read_data = vec![0; PAGE_SIZE];
                        if let Err(e) = disk_manager_clone
                            .read_page_async(request.page_id, &mut read_data)
                            .await
                        {
                            error!(error = %e, "Failed to read from disk");
                        }

                        if let Some(sender) = request.read_data_sender {
                            trace!("Sending back read data");
                            let _ = sender.send(read_data).await;
                        }
                    }
                    if let Some(sender) = request.completion_signal.take() {
                        trace!("Sending completion signal");
                        let _ = sender.send(());
                    }
                });
            }
            trace!("DiskScheduler worker loop ended");
        });

        // Initialize the write buffer and flush interval
        let write_buffer = Arc::new(Mutex::new(Vec::with_capacity(Self::MAX_BUFFER_SIZE)));
        let flush_interval = Duration::from_secs(Self::FLUSH_INTERVAL);
        let last_flush = Mutex::new(Instant::now());

        let scheduler = Arc::new(Self {
            disk_manager,
            sender,
            write_buffer,
            flush_interval,
            last_flush,
        });

        // Start the flush task
        scheduler.start_flush_task();

        scheduler
    }

    async fn flush_write_buffer(&self) {
        // Scope for the lock
        {
            let mut buffer = self.write_buffer.lock();

            if !buffer.is_empty() {
                // Clone data or prepare a list of operations to be performed outside the lock
                let requests = buffer.drain(..).collect::<Vec<_>>();

                // Explicitly drop the buffer to release the lock
                drop(buffer);

                // Process the requests outside the lock
                for mut request in requests {
                    trace!(page_id = request.page_id, "Writing to disk");
                    if let Err(e) = self
                        .disk_manager
                        .write_page_async(request.page_id, &request.data)
                        .await
                    {
                        error!(error = %e, "Failed to write to disk");
                    }
                    request.complete().await;
                }
            }
        }

        // Update the last flush time
        *self.last_flush.lock() = Instant::now();
    }

    pub async fn schedule(
        &self,
        request: DiskRequest,
    ) -> Result<(), mpsc::error::SendError<DiskRequest>> {
        self.sender.send(request).await
    }

    pub async fn batch_write(&self, batch: Vec<(PageId, Vec<u8>)>) -> Result<()> {
        let mut requests = Vec::with_capacity(batch.len());

        for (page_id, data) in batch {
            let (tx, _rx) = oneshot::channel();
            let request = DiskRequest::new(true, data, page_id.into(), Some(tx), None, 0);
            requests.push(request);
        }

        for request in &requests {
            self.schedule(request.clone()).await?;
        }

        for mut request in requests {
            request.complete().await;
        }

        Ok(())
    }

    pub fn start_flush_task(self: &Arc<Self>) {
        let flush_interval = self.flush_interval;
        let write_buffer = self.write_buffer.clone();
        let disk_manager = self.disk_manager.clone();
        let mut requests = Vec::<DiskRequest>::new();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(flush_interval).await;

                // Scope to hold the lock
                {
                    let mut buffer = write_buffer.lock();

                    if !buffer.is_empty() {
                        // Move the requests out of the buffer into a local variable
                        requests = std::mem::replace(&mut *buffer, Vec::new());
                        // MutexGuard is dropped here when `buffer` goes out of scope
                    }
                }

                // Now we can process the requests outside of the lock
                for request in &mut requests {
                    if request.is_write {
                        if let Err(e) = disk_manager
                            .write_page_async(request.page_id, &request.data)
                            .await
                        {
                            error!(error = %e, "Failed to write to disk");
                        }
                        request.complete().await;
                    } else {
                        warn!("Read requests should not be buffered");
                    }
                }
            }
        });
    }

    // #[instrument(name = "Scheduler::schedule_write", skip(self, data))]
    // pub async fn schedule_write(&self, page_id: u32, data: Vec<u8>) -> anyhow::Result<()> {
    //     info!(page_id, data_len = data.len(), "Scheduling write request");
    //     let (tx, rx) = oneshot::channel();
    //     let request = DiskRequest::new(true, data, page_id, tx, None, 0);
    //     self.schedule(request)
    //         .await
    //         .map_err(DiskSchedulerError::from)?;
    //     rx.await.map_err(DiskSchedulerError::from)?;
    //     Ok(())
    // }
    #[instrument(name = "Scheduler::schedule_write", skip(self, data, strategy))]
    pub async fn schedule_write(
        &self,
        page_id: PageId,
        data: Vec<u8>,
        strategy: WriteStrategy,
    ) -> anyhow::Result<()> {
        match strategy {
            WriteStrategy::Immediate => self.immediate_write(page_id, data).await,
            WriteStrategy::Buffered => self.buffered_write(page_id, data).await,
        }
    }

    #[instrument(name = "Scheduler::buffered_write", skip(self, data))]
    pub async fn buffered_write(&self, page_id: PageId, data: Vec<u8>) -> anyhow::Result<()> {
        let page_id = page_id.into();
        info!(page_id, data_len = data.len(), "Buffering write request");

        let request = DiskRequest::new(true, data, page_id, None, None, 0); // No completion signal

        let mut buffer = self.write_buffer.lock();

        buffer.push(request);

        if buffer.len() >= Self::MAX_BUFFER_SIZE {
            drop(buffer); // Explicitly drop the lock before flush
            self.flush_write_buffer().await;
        }

        Ok(())
    }

    #[instrument(name = "Scheduler::immediate_write", skip(self, data))]
    pub async fn immediate_write(&self, page_id: PageId, data: Vec<u8>) -> anyhow::Result<()> {
        let page_id = page_id.into();

        info!(
            page_id,
            data_len = data.len(),
            "Scheduling immediate write request"
        );

        let (tx, rx) = oneshot::channel();
        let request = DiskRequest::new(true, data, page_id, Some(tx), None, 0);

        self.schedule(request)
            .await
            .map_err(DiskSchedulerError::from)?;

        rx.await.map_err(DiskSchedulerError::from)?;

        Ok(())
    }

    #[instrument(name = "Scheduler::schedule_read", skip(self))]
    pub async fn schedule_read(&self, page_id: u32) -> anyhow::Result<Vec<u8>> {
        info!(page_id, "Scheduling read request");
        let (tx, rx) = oneshot::channel();
        let (read_tx, mut read_rx) = mpsc::channel(1);
        let request = DiskRequest::new(
            false,
            vec![0; PAGE_SIZE],
            page_id,
            Some(tx),
            Some(read_tx),
            0,
        );
        self.schedule(request)
            .await
            .map_err(DiskSchedulerError::from)?;
        rx.await.map_err(DiskSchedulerError::from)?;
        read_rx.recv().await.ok_or_else(|| {
            DiskSchedulerError::DiskManagerError("Failed to receive read data".to_string()).into()
        })
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;

    #[tokio::test]
    async fn test_schedule_write_request() {
        let (dm, _temp_dir) = setup_dm();
        let scheduler = DiskScheduler::new(dm.clone());
        let (tx, rx) = oneshot::channel();

        let data = vec![1, 2, 3, 4];
        let request = DiskRequest::new(true, data.clone(), 0, Some(tx), None, 0);

        eprintln!("Scheduling write request");
        scheduler
            .schedule(request)
            .await
            .expect("Failed to schedule request");

        eprintln!("Waiting for completion signal");
        assert!(
            rx.await.is_ok(),
            "Write request should complete successfully"
        );
        let mut buf = vec![0; PAGE_SIZE];
        let _ = dm.read_page(0, &mut buf).expect("Failed to read page");
        assert_eq!(
            buf[0..data.len()],
            data[..],
            "Data should be written to disk"
        );

        eprintln!("Test completed successfully");
    }

    #[tokio::test]
    async fn test_schedule_read_request() {
        let (dm, _temp_dir) = setup_dm();

        let data = vec![1, 2, 3, 4];
        let _ = dm.write_page(0, &data).expect("Failed to write page");
        // Get the data from disk
        let mut buf = vec![0; PAGE_SIZE];
        let _ = dm.read_page(0, &mut buf).expect("Failed to read page");
        assert_eq!(
            buf[0..data.len()],
            data[..],
            "Data should be written to disk"
        );
        assert_eq!(
            dm.num_pages(),
            1,
            "Disk manager should contain one page of data"
        );

        let scheduler = DiskScheduler::new(dm);
        let (tx, rx) = oneshot::channel();
        let (read_tx, mut read_rx) = mpsc::channel(1);

        let request = DiskRequest::new(false, vec![0; PAGE_SIZE], 0, Some(tx), Some(read_tx), 0);

        eprintln!("Scheduling read request");
        scheduler
            .schedule(request)
            .await
            .expect("Failed to schedule request");

        eprintln!("Waiting for completion signal");

        assert!(
            rx.await.is_ok(),
            "Read request should complete successfully"
        );

        // Receive the read data
        if let Some(read_data) = read_rx.recv().await {
            assert_eq!(
                &read_data[0..data.len()],
                &data[..],
                "Data should be read from disk"
            );
        } else {
            panic!("Failed to receive read data");
        }

        eprintln!("Test completed successfully");
    }

    #[tokio::test]
    async fn test_buffering_logic() {
        let (dm, _temp_dir) = setup_dm();
        // let dm =
        //     Arc::new(DiskManager::new(TEST_SCHEDULE_DB).expect("Failed to create disk manager"));
        let scheduler = DiskScheduler::new(dm.clone());

        let data = vec![1, 2, 3, 4];
        scheduler
            .schedule_write(PageId::from(0), data.clone(), WriteStrategy::Buffered)
            .await
            .unwrap();

        // Check if the buffer size is correct
        let buffer = scheduler.write_buffer.lock();
        assert_eq!(buffer.len(), 1, "Buffer should contain one request");
    }

    #[tokio::test]
    async fn test_flush_mechanism() {
        let (dm, _temp_dir) = setup_dm();
        // setup();
        // let dm = Arc::new(
        //     DiskManager::new("testdata/test_schedule.db").expect("Failed to create disk manager"),
        // );
        let scheduler = DiskScheduler::new(dm.clone());

        let data = vec![1, 2, 3, 4];
        scheduler
            .schedule_write(PageId::from(0), data.clone(), WriteStrategy::Buffered)
            .await
            .unwrap();

        // Force flush
        scheduler.flush_write_buffer().await;

        // Check if the buffer is empty after flushing
        let buffer = scheduler.write_buffer.lock();
        assert!(buffer.is_empty(), "Buffer should be empty after flushing");

        // Verify that data is written to disk...
        let mut buf = vec![0; PAGE_SIZE];
        let _ = dm.read_page(0, &mut buf).expect("Failed to read page");
        assert_eq!(buf[0..4], [1, 2, 3, 4], "Data should be written to disk");
    }

    #[tokio::test]
    async fn test_schedule_write_coalescing() {
        let (dm, _temp_dir) = setup_dm();
        let scheduler = DiskScheduler::new(dm.clone());

        let data1 = vec![1, 2, 3, 4];
        let data2 = vec![5, 6, 7, 8];

        // Schedule two write requests
        scheduler
            .schedule_write(PageId::from(0), data1.clone(), WriteStrategy::Buffered)
            .await
            .unwrap();
        scheduler
            .schedule_write(PageId::from(1), data2.clone(), WriteStrategy::Buffered)
            .await
            .unwrap();

        // Force flush to ensure that requests are processed
        scheduler.flush_write_buffer().await;

        // Verify that data is written to disk...
        let mut buf = vec![0; PAGE_SIZE];
        let _ = dm.read_page(0, &mut buf).expect("Failed to read page");
        assert_eq!(
            buf[0..data1.len()],
            data1[..],
            "Data should be written to disk"
        );
        let _ = dm.read_page(1, &mut buf).expect("Failed to read page");
        assert_eq!(
            buf[0..data2.len()],
            data2[..],
            "Data should be written to disk"
        );
    }

    #[tokio::test]
    async fn test_buffered_write() {
        let (dm, _temp_dir) = setup_dm();
        let scheduler = DiskScheduler::new(dm.clone());

        // Schedule a buffered write
        scheduler
            .buffered_write(PageId::from(0), vec![1, 2, 3, 4])
            .await
            .unwrap();

        // Check buffer size
        let buffer = scheduler.write_buffer.lock();
        assert_eq!(buffer.len(), 1, "Buffer should contain one request");
        drop(buffer); // Explicitly drop the lock

        // Force flush
        scheduler.flush_write_buffer().await;

        // Check if the buffer is empty after flushing
        let buffer = scheduler.write_buffer.lock();
        assert!(buffer.is_empty(), "Buffer should be empty after flushing");

        // Verify that data is written to disk...
        let mut buf = vec![0; PAGE_SIZE];
        let _ = dm.read_page(0, &mut buf).expect("Failed to read page");
        assert_eq!(buf[0..4], [1, 2, 3, 4], "Data should be written to disk");
    }
}

#[cfg(test)]
mod high_level_api_tests {
    use super::*;

    #[tokio::test]
    async fn test_high_level_write_api() {
        let (dm, _temp_dir) = setup_dm();
        let scheduler = DiskScheduler::new(dm.clone());

        let data = vec![1, 2, 3, 4];
        scheduler
            .schedule_write(PageId::from(0), data.clone(), WriteStrategy::Immediate)
            .await
            .unwrap();

        // Assert data is written to disk...
        let mut buf = vec![0; PAGE_SIZE];
        let _ = dm.read_page(0, &mut buf).expect("Failed to read page");
        assert_eq!(
            buf[0..data.len()],
            data[..],
            "Data should be written to disk"
        );
    }

    #[tokio::test]
    async fn test_high_level_read_api() {
        let (dm, _temp_dir) = setup_dm();

        // Write data to disk
        let data = vec![1, 2, 3, 4];
        let _ = dm.write_page(0, &data).expect("Failed to write page");
        assert_eq!(
            dm.num_pages(),
            1,
            "Disk manager should contain one page of data"
        );

        let scheduler = DiskScheduler::new(dm);

        let read_data = scheduler
            .schedule_read(0)
            .await
            .expect("Failed to read page");

        assert_eq!(read_data.len(), PAGE_SIZE, "Read data should be a page");
        assert_eq!(
            &read_data[0..data.len()],
            &data[..],
            "Data should be read from disk"
        );
    }
}

// #[tokio::test]
// async fn test_data_integrity() {
//     let dm =
//         Arc::new(DiskManager::new("testdata/test.db").expect("Failed to create disk manager"));
//     let scheduler = DiskScheduler::new(dm.clone());

//     let data = vec![1, 2, 3, 4];
//     scheduler.schedule_write(0, data.clone()).await.unwrap();
//     scheduler.flush_write_buffer().await;

//     // TODO: Read data from disk and compare
//     // ...
// }

// #[cfg(test)]
// mod priority_tests {
//     use super::*;
//     use tokio::sync::oneshot;

//     #[tokio::test]
//     #[ignore = "TODO: Implement priority scheduling"]
//     async fn test_request_prioritization() {
//         let dm = Arc::new(DiskManager::new("testdata/test_schedule.db").unwrap());
//         let scheduler = DiskScheduler::new(dm);

//         // Priorities for the requests
//         let priorities = vec![1, 3, 2];

//         // TODO: Further flesh out priority tests
//     }
// }
