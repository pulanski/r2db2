use common::PageId;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, VecDeque},
    time::Instant,
};

pub struct LRUKReplacer {
    // Maps page_id to a deque of timestamps of the last k accesses
    access_history: DashMap<PageId, VecDeque<Instant>>,
    // Global ordering of accesses (for efficiently finding the least recently used)
    global_order: RwLock<BTreeMap<Instant, PageId>>,
    k: usize,
}
