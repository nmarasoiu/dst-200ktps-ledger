//! The liveness and timeliness properties of README section 11, checked once faults stop. Unlike the safety
//! invariants they need the time of each event and the point where faults stopped. Port of `Liveness.scala`.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::vec::Vec;

use crate::domain::{CommandId, Record, RefusalReason, Reply, Request};
use crate::history::{CallerRecord, History, Violation};
use crate::ids::ClientKey;
use crate::machine::{HistoryTime, Span};
use crate::partitioner::Partition;

pub const ANSWERED: &str = "Every request that reaches a live edge instance gets an answer within the edge deadline";
pub const COMMAND_HAS_OUTCOME: &str = "Every command in the log gets an outcome in the log";
pub const CANCEL_ACKNOWLEDGED: &str = "Every cancel is acknowledged";
pub const HOLD_ENDS: &str = "Every hold ends: captured, voided, or expired by TigerBeetle within its timeout plus the expiry lag";
pub const THROUGHPUT_FLOOR: &str = "Under overload, useful throughput stays above a floor";
pub const REFUSALS_STOP: &str = "Once faults and overload have stopped, no edge refuses for overload";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// The edge deadline, on each edge instance's own clock.
    pub deadline: Span,
    /// The edge deadline plus the network between the caller and the edge, on the history clock.
    pub answer_within: Span,
    /// The deadline plus the resolver's grace period, plus slack.
    pub outcome_within: Span,
    /// How long after faults stop before requests must be answered in time again.
    pub settle: Span,
    pub expiry_lag: Span,
}

pub fn check(h: &History, b: &Bounds) -> Vec<Violation> {
    let mut found = answered_in_time(h, b);
    found.extend(every_command_has_outcome(h, b));
    found.extend(every_cancel_acknowledged(h));
    found.extend(every_hold_ends(h, b));
    found
}

/// By each edge instance's own clock when the history has the edges' records, as README section 11 states it;
/// otherwise from the caller's side, with an allowance for the network.
pub fn answered_in_time(h: &History, b: &Bounds) -> Vec<Violation> {
    if h.edges.is_empty() { answered_by_caller_clock(h, b) } else { answered_by_edge_clock(h, b) }
}

/// Requests that reached an edge once faults had stopped and settled. With faults stopped no instance crashes, so
/// every one of them must be answered.
fn answered_by_edge_clock(h: &History, b: &Bounds) -> Vec<Violation> {
    h.edges
        .iter()
        .filter(|e| judged(h, e.at, b) && e.answered.as_ref().is_none_or(|(t, _)| *t > e.arrived + b.deadline))
        .map(|e| {
            let detail = format!("{:?} at edge {} was not answered within {} by its clock", e.request, e.instance, b.deadline);
            Violation::new(ANSWERED, detail)
        })
        .collect()
}

fn answered_by_caller_clock(h: &History, b: &Bounds) -> Vec<Violation> {
    let mut received: BTreeMap<&Request, Vec<HistoryTime>> = BTreeMap::new();
    for r in &h.caller {
        if let CallerRecord::Received(at, request, _) = r {
            received.entry(request).or_default().push(*at);
        }
    }
    h.caller
        .iter()
        .filter_map(|r| match r {
            CallerRecord::Sent(at, request) if judged(h, *at, b) => {
                let answered = received.get(request).is_some_and(|ts| ts.iter().any(|t| *t >= *at && *t <= *at + b.answer_within));
                (!answered).then(|| {
                    let detail = format!("{request:?} sent at {at} had no answer within {}", b.answer_within);
                    Violation::new(ANSWERED, detail)
                })
            }
            _ => None,
        })
        .collect()
}

/// Sent once faults have stopped and settled, early enough that the run could see the answer.
fn judged(h: &History, at: HistoryTime, b: &Bounds) -> bool {
    h.faults_stopped_at.is_none_or(|stopped| at >= stopped + b.settle) && at + b.answer_within <= h.ended_at
}

/// An outcome later on the same partition, for every command logged early enough to have been resolved.
pub fn every_command_has_outcome(h: &History, b: &Bounds) -> Vec<Violation> {
    let mut outcomes: BTreeMap<(Partition, CommandId), Vec<i64>> = BTreeMap::new();
    for r in &h.log {
        if let Record::Outcome(c, _, _) = &r.record {
            outcomes.entry((r.partition, c.id())).or_default().push(r.offset.value());
        }
    }
    h.log
        .iter()
        .filter_map(|r| match &r.record {
            Record::Command(c) if r.at + b.outcome_within <= h.ended_at => {
                let id = c.id();
                let later = outcomes.get(&(r.partition, id)).is_some_and(|os| os.iter().any(|o| *o > r.offset.value()));
                (!later).then(|| {
                    let detail = format!("{id:?} at partition {}, offset {} has no outcome", r.partition.index(), r.offset.value());
                    Violation::new(COMMAND_HAS_OUTCOME, detail)
                })
            }
            _ => None,
        })
        .collect()
}

pub fn every_cancel_acknowledged(h: &History) -> Vec<Violation> {
    let mut sent: Vec<&ClientKey> = Vec::new();
    for r in &h.caller {
        if let CallerRecord::Sent(_, Request::Cancel(key)) = r
            && !sent.contains(&key)
        {
            sent.push(key);
        }
    }
    let answered: BTreeSet<&ClientKey> = h
        .caller
        .iter()
        .filter_map(|r| match r {
            CallerRecord::Received(_, Request::Cancel(key), Reply::Decided(_)) => Some(key),
            _ => None,
        })
        .collect();
    sent.into_iter()
        .filter(|key| !answered.contains(key))
        .map(|key| Violation::new(CANCEL_ACKNOWLEDGED, format!("the cancel for {key:?} was never acknowledged")))
        .collect()
}

/// A scan cannot see expiries, so expiry is checked only when the run observed it, as the simulator does.
pub fn every_hold_ends(h: &History, b: &Bounds) -> Vec<Violation> {
    let ledger = &h.ledger;
    let mut found = Vec::new();
    for hold in ledger.transfers().iter().filter(|t| t.transfer.is_pending()) {
        let ended = !ledger.posts(hold.id()).is_empty() || !ledger.voids(hold.id()).is_empty();
        match (ended, hold.expires_at(), ledger.expiries()) {
            (true, _, _) => {}
            (false, None, _) => found.push(Violation::new(HOLD_ENDS, format!("hold {} has no timeout and never ended", hold.id()))),
            (false, Some(at), Some(seen)) => {
                // Checked once faults stop, so a hold that came due while they ran has until then.
                let due = ledger.healed_at().get(&hold.cluster).map_or(at, |healed| at.max(*healed)) + b.expiry_lag.as_nanos();
                match seen.get(&hold.id()) {
                    Some(expired_at) if *expired_at > due => {
                        found.push(Violation::new(HOLD_ENDS, format!("hold {} expired at {expired_at}, after {due}", hold.id())));
                    }
                    None if ledger.scanned_at().get(&hold.cluster).is_some_and(|scan| *scan >= due) => {
                        found.push(Violation::new(HOLD_ENDS, format!("hold {} was still held at the scan, past {due}", hold.id())));
                    }
                    _ => {}
                }
            }
            (false, Some(_), None) => {}
        }
    }
    found
}

/// A refusal for overload that a caller heard from this time on.
pub fn refusals_stop(h: &History, from: HistoryTime) -> Vec<Violation> {
    h.caller
        .iter()
        .filter_map(|r| match r {
            CallerRecord::Received(at, request, Reply::Refused(RefusalReason::Overload)) if *at >= from => {
                Some(Violation::new(REFUSALS_STOP, format!("{request:?} was refused for overload at {at}, after {from}")))
            }
            _ => None,
        })
        .collect()
}

/// Decisions answered per window across an overload, each at least the floor.
pub fn throughput_floor(h: &History, from: HistoryTime, to: HistoryTime, window: Span, floor: usize) -> Vec<Violation> {
    let w = window.as_millis();
    let decided: Vec<i64> = h
        .caller
        .iter()
        .filter_map(|r| match r {
            CallerRecord::Received(at, _, Reply::Decided(_)) if *at >= from && *at < to => Some(at.millis()),
            _ => None,
        })
        .collect();
    let mut found = Vec::new();
    let mut start = from.millis();
    while start <= to.millis() - w {
        let n = decided.iter().filter(|at| **at >= start && **at < start + w).count();
        if n < floor {
            found.push(Violation::new(THROUGHPUT_FLOOR, format!("{n} decisions in [{start}, {}), below {floor}", start + w)));
        }
        start += w;
    }
    found
}
