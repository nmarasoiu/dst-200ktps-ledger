//! Admission control at the edge (README section 12, gap 1): refuse authorizations fast once the edge is overloaded
//! or the authorization's log partition has no leader. A refusal is cheap, since nothing was sent, and the caller may
//! resend with the same key. Captures are told to retry. Cancels release money, so they are accepted under more load
//! than anything else, but not under any load: a call that misses the deadline is resent while it still queues, and
//! each resend lengthens the queue that made it miss. Port of `Admission.scala`.
//!
//! What counts as overloaded is learned, since what the ledger can serve is not known in advance and changes. The
//! limit on requests in flight falls with each slow reply from the ledger and rises slowly with each fast one, so an
//! instance finds its share of the ledger whatever the other instances do. With nothing in flight one request always
//! goes, and its reply is the news that the ledger is back.
//!
//! The arithmetic on the limit is IEEE double precision, as on the JVM, so both cores reach the same limits.

use alloc::collections::BTreeSet;
use alloc::vec;

use crate::domain::{RefusalReason, Request};
use crate::machine::{Span, Step};
use crate::mark::Mark;
use crate::partitioner::{self, Partition};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Config {
    /// The most the limit can reach.
    pub max_in_flight: i32,
    /// A reply slower than this means the queue is too long. It sits well inside the deadline.
    pub max_ledger_latency: Span,
    pub partitions: u32,
    /// Where the limit starts: a cold instance has no evidence that the ledger can take more.
    pub start_in_flight: i32,
    /// How far past the limit a cancel is still accepted, as a multiple of it.
    pub cancel_headroom: f64,
}

impl Config {
    /// With the Scala defaults: a limit that starts at 16, and cancels accepted up to twice it.
    pub const fn new(max_in_flight: i32, max_ledger_latency: Span, partitions: u32) -> Config {
        Config { max_in_flight, max_ledger_latency, partitions, start_in_flight: 16, cancel_headroom: 2.0 }
    }
}

/// `limit` is how many requests this instance lets in flight right now, between 1 and `max_in_flight`.
#[derive(Clone, Debug, PartialEq)]
pub struct State {
    pub in_flight: i32,
    pub limit: f64,
    pub leaderless: BTreeSet<Partition>,
}

impl State {
    pub fn initial(cfg: &Config) -> State {
        State { in_flight: 0, limit: f64::from(cfg.start_in_flight).min(f64::from(cfg.max_in_flight)), leaderless: BTreeSet::new() }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Arrived(Request),
    /// An admitted request has ended, and its ledger call, if it made one, has replied.
    Finished,
    LedgerLatency(Span),
    /// The partitions the producer's metadata shows without a leader (README section 8, failover).
    LeadersChanged(BTreeSet<Partition>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Decision {
    Admit,
    Refuse(RefusalReason),
    Retry,
}

/// What one slow reply leaves of the limit. A queue of slow replies takes it nearly to one, since a long queue must
/// stop growing now: every call in it that misses the deadline is resent. Fast replies soon bring it back.
const CUT: f64 = 0.9;

pub fn step(s: State, e: Event, cfg: &Config) -> Step<State, Decision> {
    match e {
        Event::Arrived(request) => {
            let no_leader = s.leaderless.contains(&partitioner::partition(request.a(), cfg.partitions));
            let in_flight = f64::from(s.in_flight);
            let overloaded = in_flight >= s.limit;
            let saturated = in_flight >= s.limit * cfg.cancel_headroom;
            match request {
                Request::Authorize(_) if no_leader => {
                    Step::with(s, vec![Decision::Refuse(RefusalReason::NoLeader)], vec![Mark::AdmissionRefusedNoLeader])
                }
                Request::Authorize(_) if overloaded => {
                    Step::with(s, vec![Decision::Refuse(RefusalReason::Overload)], vec![Mark::AdmissionRefusedOverload])
                }
                _ if no_leader => Step::with(s, vec![Decision::Retry], vec![Mark::AdmissionRetryNoLeader]),
                Request::Capture(..) if overloaded => Step::with(s, vec![Decision::Retry], vec![Mark::AdmissionRetryOverload]),
                Request::Cancel(_) if saturated => Step::with(s, vec![Decision::Retry], vec![Mark::AdmissionRetrySaturated]),
                Request::Cancel(_) if overloaded => admit(s).mark(Mark::AdmissionCancelDuringOverload),
                _ => admit(s),
            }
        }
        Event::Finished => Step::new(State { in_flight: (s.in_flight - 1).max(0), ..s }),
        Event::LedgerLatency(observed) => {
            if observed >= cfg.max_ledger_latency {
                Step::new(State { limit: (s.limit * CUT).max(1.0), ..s })
            } else if f64::from(s.in_flight * 2) < s.limit {
                // A fast reply says nothing about a limit the instance is not using, so it only raises one in use. Far
                // inside the target the queue is nearly empty, so one more for each reply is safe; near it, one more
                // for each limit's worth of replies.
                Step::new(s)
            } else {
                let more = if observed < cfg.max_ledger_latency / 4 { 1.0 } else { 1.0 / s.limit };
                Step::new(State { limit: (s.limit + more).min(f64::from(cfg.max_in_flight)), ..s })
            }
        }
        Event::LeadersChanged(leaderless) => Step::new(State { leaderless, ..s }),
    }
}

fn admit(s: State) -> Step<State, Decision> {
    Step::with(State { in_flight: s.in_flight + 1, ..s }, vec![Decision::Admit], vec![])
}
