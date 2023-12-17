use common::{Timestamp, TxnId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionState {
    Running,
    Tainted,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    SnapshotIsolation,
    Serializable,
}

pub struct Transaction {
    isolation_level: IsolationLevel,
    thread_id: ThreadId,
    txn_id: TxnId,
    state: Arc<Mutex<TransactionState>>,
    read_ts: Arc<Mutex<Timestamp>>,
    commit_ts: Arc<Mutex<Timestamp>>,
}

impl Transaction {
    // TODO: implement methods and functionality
}
