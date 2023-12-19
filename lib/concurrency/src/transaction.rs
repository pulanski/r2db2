#![allow(dead_code)]

use anyhow::Result;
use common::rid::RID;
use common::{Timestamp, TxnId};
use getset::{Getters, Setters};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::thread::ThreadId;
use thiserror::Error;
use tracing::{error, info};
use typed_builder::TypedBuilder;

type TableOid = u32;
type LogIndex = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AbstractExpressionRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TransactionState {
    Running,
    Tainted,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    SnapshotIsolation,
    Serializable,
}

#[derive(Debug, Clone, Copy)]
pub struct UndoLink {
    prev_txn: Option<TxnId>,
    prev_log_idx: usize,
}

impl UndoLink {
    pub fn new(prev_txn: Option<TxnId>, prev_log_idx: usize) -> Self {
        UndoLink {
            prev_txn,
            prev_log_idx,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.prev_txn.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct UndoLog {
    is_deleted: bool,
    // ... other fields
}

#[derive(Debug, Error, Clone)]
pub enum TransactionError {
    #[error("Invalid access of transaction")]
    InvalidAccess,
    // TODO: impl more to turn on these errors
    // #[error("Transaction aborted")]
    // Aborted,
    // #[error("Transaction already committed")]
    // AlreadyCommitted,
    // #[error("Transaction already aborted")]
    // AlreadyAborted,
    // #[error("Transaction already running")]
    // AlreadyRunning,
    // #[error("Transaction already tainted")]
    // AlreadyTainted,
    // #[error("Transaction not running")]
    // NotRunning,
    // #[error("Transaction not tainted")]
    // NotTainted,
    // #[error("Transaction not committed")]
    // NotCommitted,
    // #[error("Transaction not aborted")]
    // NotAborted,
    // #[error("Transaction not found")]
    // NotFound,
    // #[error("Transaction not serializable")]
    // NotSerializable,
    // #[error("Transaction not read uncommitted")]
    // NotReadUncommitted,
    // #[error("Transaction not snapshot isolation")]
    // NotSnapshotIsolation,
    // #[error("Transaction not writable")]
    // NotWritable,
    // #[error("Transaction not readable")]
    // NotReadable,
}

#[derive(
    Debug,
    Clone,
    // Serialize, Deserialize, TODO: add serde support (need to fix issues with RwLock)
    Getters,
    Setters,
    TypedBuilder,
)]
#[getset(get = "pub", set = "pub")]
pub struct Transaction {
    isolation_level: IsolationLevel,
    thread_id: ThreadId,
    txn_id: TxnId,
    state: Arc<RwLock<TransactionState>>,
    read_ts: Arc<RwLock<Timestamp>>,
    commit_ts: Arc<RwLock<Timestamp>>,
    undo_logs: Arc<RwLock<Vec<UndoLog>>>,
    write_set: Arc<RwLock<HashMap<TableOid, HashSet<RID>>>>,
    scan_predicates: Arc<RwLock<HashMap<TableOid, Vec<AbstractExpressionRef>>>>,
}

impl PartialEq for Transaction {
    fn eq(&self, other: &Self) -> bool {
        self.txn_id == other.txn_id
    }
}

impl Transaction {
    pub fn new(isolation_level: IsolationLevel, txn_id: TxnId) -> Self {
        info!(
            "Creating new transaction with id {} and isolation level {:?}",
            txn_id, isolation_level
        );

        let state = Arc::new(RwLock::new(TransactionState::Running));
        let read_ts = Arc::new(RwLock::new(Timestamp::default()));
        let commit_ts = Arc::new(RwLock::new(Timestamp::default()));
        let undo_logs = Arc::new(RwLock::new(Vec::new()));
        let write_set = Arc::new(RwLock::new(HashMap::new()));
        let scan_predicates = Arc::new(RwLock::new(HashMap::new()));

        Transaction::builder()
            .isolation_level(isolation_level)
            .thread_id(std::thread::current().id())
            .txn_id(txn_id)
            .state(state)
            .read_ts(read_ts)
            .commit_ts(commit_ts)
            .undo_logs(undo_logs)
            .write_set(write_set)
            .scan_predicates(scan_predicates)
            .build()
    }

    // Modify an existing undo log
    pub fn modify_undo_log(
        &self,
        log_idx: usize,
        new_log: UndoLog,
    ) -> Result<(), TransactionError> {
        let mut undo_logs = self.undo_logs.write();
        if log_idx > undo_logs.len() {
            error!("Invalid log index");
            return Err(TransactionError::InvalidAccess);
        }

        undo_logs[log_idx] = new_log;

        Ok(())
    }

    pub fn append_undo_log(&self, log: UndoLog) -> LogIndex {
        let mut undo_logs = self.undo_logs.write();

        undo_logs.push(log);
        undo_logs.len() - 1
    }
    // TODO: implement methods and functionality
}

impl Default for Transaction {
    fn default() -> Self {
        Self {
            isolation_level: IsolationLevel::Serializable,
            thread_id: std::thread::current().id(),
            txn_id: TxnId::default(),
            state: Arc::new(RwLock::new(TransactionState::Running)),
            read_ts: Arc::new(RwLock::new(Timestamp::default())),
            commit_ts: Arc::new(RwLock::new(Timestamp::default())),
            undo_logs: Arc::new(RwLock::new(Vec::new())),
            scan_predicates: Arc::new(RwLock::new(HashMap::new())),
            write_set: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Transaction {{ id: {}, state: {:?} }}",
            self.txn_id,
            *self.state.read()
        )
    }
}

impl Eq for Transaction {}

impl PartialOrd for Transaction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.txn_id.partial_cmp(&other.txn_id)
    }
}

impl Ord for Transaction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.txn_id.cmp(&other.txn_id)
    }
}
