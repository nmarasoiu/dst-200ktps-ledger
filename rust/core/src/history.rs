//! One run's history: the one format the simulator and the stage 6 workload driver write and every invariant reads
//! (README section 11, one history for every layer). Port of `History.scala`.
//!
//! It has three sources: what each caller sent and received, what the edges and the resolver wrote to the command
//! log, and what TigerBeetle holds. The simulator adds a fourth, what each edge instance saw of each request by its
//! own clock, for the deadline property. `at` times are on the history's one clock, with one exception in real runs:
//! a log record's time is its append time, from a partition leader's clock, and `log_clock_slack` says how far that
//! may be from the history's. TigerBeetle's timestamps stay in its own nanoseconds, per cluster.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use crate::domain::{AccountId, Record, Reply, Request};
use crate::ids::TransferId;
use crate::machine::{EdgeTime, HistoryTime, Span};
use crate::partitioner::{Offset, Partition};
use crate::periods::Period;
use crate::tb::Transfer;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct History {
    pub caller: Vec<CallerRecord>,
    pub log: Vec<LogRecord>,
    pub edges: Vec<EdgeRecord>,
    pub ledger: LedgerSnapshot,
    /// When the simulator healed every fault, or the fault scenario ended; None if there were no faults.
    pub faults_stopped_at: Option<HistoryTime>,
    pub ended_at: HistoryTime,
    /// Zero in the simulator, which has one clock.
    pub log_clock_slack: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallerRecord {
    Sent(HistoryTime, Request),
    Received(HistoryTime, Request, Reply),
    /// A connection error, or no answer at all.
    Lost(HistoryTime, Request),
}

impl CallerRecord {
    pub fn at(&self) -> HistoryTime {
        match self {
            CallerRecord::Sent(at, _) | CallerRecord::Received(at, _, _) | CallerRecord::Lost(at, _) => *at,
        }
    }

    pub fn request(&self) -> &Request {
        match self {
            CallerRecord::Sent(_, r) | CallerRecord::Received(_, r, _) | CallerRecord::Lost(_, r) => r,
        }
    }
}

/// A command log record. `at` is when it landed, whatever its writer heard: the simulator's time, or in real runs its
/// append time, within the history's `log_clock_slack`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub partition: Partition,
    pub offset: Offset,
    pub at: HistoryTime,
    pub record: Record,
}

/// What one edge instance saw of one request, by its own clock: when the request arrived and, unless the instance
/// crashed first, when and how it was answered. `at` is the arrival in history time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeRecord {
    pub instance: i32,
    pub at: HistoryTime,
    pub request: Request,
    pub arrived: EdgeTime,
    pub answered: Option<(EdgeTime, Reply)>,
}

/// A transfer from the scan of one cluster, as TigerBeetle recorded it, with its timestamp in nanoseconds. A post or
/// a void carries the hold's accounts, ledger and code, and the amount it moved, never the maximum a full capture
/// sends (README section 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LedgerTransfer {
    pub cluster: Period,
    pub transfer: Transfer,
    pub timestamp: i64,
}

impl LedgerTransfer {
    pub fn id(&self) -> TransferId {
        self.transfer.id
    }

    /// When TigerBeetle expires the hold, if it is a pending transfer with a timeout.
    pub fn expires_at(&self) -> Option<i64> {
        if self.transfer.is_pending() && self.transfer.timeout_seconds > 0 {
            Some(self.timestamp + i64::from(self.transfer.timeout_seconds) * 1_000_000_000)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LedgerAccount {
    pub cluster: Period,
    pub id: AccountId,
    pub debits_must_not_exceed_credits: bool,
    pub credits_must_not_exceed_debits: bool,
}

/// What TigerBeetle holds at the end of a run. `scanned_at` is each cluster's timestamp at the scan. `expiries` are
/// the holds TigerBeetle expired, with when, if the run observed them, as the simulator does; a scan cannot see them,
/// so real runs infer them from each hold's timestamp and timeout (README section 12, gap 4). `healed_at` is each
/// cluster's timestamp when faults stopped, if the run knows it: a cluster that was away expires its holds late.
///
/// The fields are private so the indexes built from them cannot go stale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerSnapshot {
    transfers: Vec<LedgerTransfer>,
    accounts: Vec<LedgerAccount>,
    scanned_at: BTreeMap<Period, i64>,
    expiries: Option<BTreeMap<TransferId, i64>>,
    healed_at: BTreeMap<Period, i64>,
    by_id: BTreeMap<TransferId, Vec<usize>>,
    by_pending: BTreeMap<TransferId, Vec<usize>>,
}

impl LedgerSnapshot {
    pub fn new(
        transfers: Vec<LedgerTransfer>,
        accounts: Vec<LedgerAccount>,
        scanned_at: BTreeMap<Period, i64>,
        expiries: Option<BTreeMap<TransferId, i64>>,
        healed_at: BTreeMap<Period, i64>,
    ) -> LedgerSnapshot {
        let mut by_id: BTreeMap<TransferId, Vec<usize>> = BTreeMap::new();
        let mut by_pending: BTreeMap<TransferId, Vec<usize>> = BTreeMap::new();
        for (i, t) in transfers.iter().enumerate() {
            by_id.entry(t.id()).or_default().push(i);
            if t.transfer.is_post() || t.transfer.is_void() {
                by_pending.entry(t.transfer.pending_id).or_default().push(i);
            }
        }
        LedgerSnapshot { transfers, accounts, scanned_at, expiries, healed_at, by_id, by_pending }
    }

    pub fn transfers(&self) -> &[LedgerTransfer] {
        &self.transfers
    }

    pub fn accounts(&self) -> &[LedgerAccount] {
        &self.accounts
    }

    pub fn scanned_at(&self) -> &BTreeMap<Period, i64> {
        &self.scanned_at
    }

    pub fn expiries(&self) -> Option<&BTreeMap<TransferId, i64>> {
        self.expiries.as_ref()
    }

    pub fn healed_at(&self) -> &BTreeMap<Period, i64> {
        &self.healed_at
    }

    fn at(&self, indexes: Option<&Vec<usize>>) -> Vec<&LedgerTransfer> {
        indexes.map_or_else(Vec::new, |is| is.iter().map(|i| &self.transfers[*i]).collect())
    }

    pub fn with_id(&self, id: TransferId) -> Vec<&LedgerTransfer> {
        self.at(self.by_id.get(&id))
    }

    pub fn pending(&self, a: TransferId) -> Option<&LedgerTransfer> {
        self.with_id(a).into_iter().find(|t| t.transfer.is_pending())
    }

    pub fn posts(&self, a: TransferId) -> Vec<&LedgerTransfer> {
        self.at(self.by_pending.get(&a)).into_iter().filter(|t| t.transfer.is_post()).collect()
    }

    pub fn voids(&self, a: TransferId) -> Vec<&LedgerTransfer> {
        self.at(self.by_pending.get(&a)).into_iter().filter(|t| t.transfer.is_void()).collect()
    }

    pub fn duplicate_ids(&self) -> Vec<(TransferId, Vec<&LedgerTransfer>)> {
        self.by_id.iter().filter(|(_, is)| is.len() > 1).map(|(id, is)| (*id, self.at(Some(is)))).collect()
    }

    pub fn closed_more_than_once(&self) -> Vec<(TransferId, Vec<&LedgerTransfer>)> {
        self.by_pending.iter().filter(|(_, is)| is.len() > 1).map(|(a, is)| (*a, self.at(Some(is)))).collect()
    }

    /// Whether a hold had expired by the scan: observed, or inferred from its timeout.
    pub fn expired(&self, hold: &LedgerTransfer) -> bool {
        match &self.expiries {
            Some(seen) => seen.contains_key(&hold.id()),
            None => hold.expires_at().is_some_and(|at| self.scanned_at.get(&hold.cluster).is_some_and(|scan| at <= *scan)),
        }
    }
}

/// A broken invariant or property, named as README section 11 states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub property: &'static str,
    pub detail: String,
}

impl Violation {
    pub fn new(property: &'static str, detail: String) -> Violation {
        Violation { property, detail }
    }
}
