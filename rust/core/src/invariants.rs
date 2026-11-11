//! The safety invariants of README section 11, as predicates over a history. They hold at every point of every run,
//! whatever the faults and the clock skew; each returns the violations it finds. Port of `Invariants.scala`.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::vec;
use alloc::vec::Vec;
use core::fmt;

use crate::domain::{
    AccountId, AuthOutcome, CancelOutcome, CaptureAmount, CaptureOutcome, CaptureRefusal, CommandId, Decision, DeclineReason, Kind,
    Outcome, Record, Reply, Request,
};
use crate::history::{CallerRecord, History, LedgerAccount, LedgerSnapshot, LedgerTransfer, LogRecord, Violation};
use crate::ids::{self, ClientKey, TransferId};
use crate::machine::HistoryTime;
use crate::merge::{self, Hold};
use crate::periods::Period;
use crate::tb::TransferFlag;
use crate::wide::Wide;

pub const NO_TRANSFER_TWICE: &str = "No transfer is applied twice";
pub const ONE_OUTCOME: &str = "No request reaches two different terminal outcomes";
pub const CANCEL_LEAVES_NO_HOLD: &str = "Once a cancel is reported cancelled, no hold survives and no capture posts it";
pub const CAPTURE_ONCE: &str = "A hold is captured at most once, for no more than it held";
pub const LOGGED_BEFORE_ANSWERED: &str =
    "Whenever the edge answered with a decision, that decision was already acknowledged in the command log";
pub const MATCHES_LEDGER: &str = "Every decision matches the ledger";
pub const REFUSAL_LEAVES_NO_HOLD: &str =
    "If a caller stopped resending an authorization after a refusal, no hold was ever created for its key";
pub const BALANCES_WITHIN_LIMITS: &str = "Balances never break their configured limits";
pub const APPROVAL_STANDS: &str =
    "With a caller that keeps its contracts, a hold it was told is approved is voided only after it sent a cancel for it";
pub const WRITES_FOLLOW_COMMANDS: &str = "Every hold, void and post in the ledger follows a command in the log";

pub fn check(h: &History) -> Vec<Violation> {
    let mut found = no_transfer_applied_twice(h);
    found.extend(no_two_outcomes(h));
    found.extend(cancel_leaves_no_hold(h));
    found.extend(captured_at_most_once(h));
    found.extend(answered_after_logged(h));
    found.extend(decisions_match_ledger(h));
    found.extend(refusal_leaves_no_hold(h));
    found.extend(balances_within_limits(h));
    found.extend(writes_follow_commands(h));
    found
}

/// `check`, and what only a caller that keeps its contracts is promised.
pub fn check_kept_contracts(h: &History) -> Vec<Violation> {
    let mut found = check(h);
    found.extend(approval_stands(h));
    found
}

/// Every outcome seen, by hold: logged by an edge or the resolver, or heard by a caller.
pub fn outcomes(h: &History) -> Vec<(TransferId, Outcome)> {
    let logged = h.log.iter().filter_map(|r| match &r.record {
        Record::Outcome(c, o, _) => Some((c.a(), *o)),
        Record::Command(_) => None,
    });
    let heard = h.caller.iter().filter_map(|r| match r {
        CallerRecord::Received(_, request, Reply::Decided(o)) => Some((request.a(), *o)),
        _ => None,
    });
    logged.chain(heard).collect()
}

pub fn holds(h: &History) -> BTreeMap<TransferId, Hold> {
    merge::fold(outcomes(h))
}

pub fn no_transfer_applied_twice(h: &History) -> Vec<Violation> {
    let twice =
        h.ledger.duplicate_ids().into_iter().map(|(id, ts)| Violation::new(NO_TRANSFER_TWICE, format!("{id} appears {} times", ts.len())));
    let closed = h
        .ledger
        .closed_more_than_once()
        .into_iter()
        .map(|(a, ts)| Violation::new(NO_TRANSFER_TWICE, format!("hold {a} was posted or voided {} times", ts.len())));
    twice.chain(closed).collect()
}

/// Over decisions, with every outcome merged by the one merge, so repeats with a generic reason pass.
pub fn no_two_outcomes(h: &History) -> Vec<Violation> {
    holds(h).values().flat_map(Hold::conflicts).map(|c| Violation::new(ONE_OUTCOME, format!("{c:?}"))).collect()
}

/// A hold reported cancelled was never created, or was voided, or had expired; and it was never posted. A later
/// approval for the same ID does not undo the cancel: the merge keeps it.
pub fn cancel_leaves_no_hold(h: &History) -> Vec<Violation> {
    let mut found = Vec::new();
    for (a, hold) in holds(h) {
        if !hold.cancel.contains(&CancelOutcome::Cancelled) {
            continue;
        }
        match h.ledger.pending(a) {
            None => {}
            Some(_) if !h.ledger.posts(a).is_empty() => {
                found.push(Violation::new(CANCEL_LEAVES_NO_HOLD, format!("hold {a} was reported cancelled and was posted")));
            }
            Some(held) if h.ledger.voids(a).is_empty() && !h.ledger.expired(held) => {
                found.push(Violation::new(CANCEL_LEAVES_NO_HOLD, format!("hold {a} was reported cancelled and survives")));
            }
            Some(_) => {}
        }
    }
    found
}

pub fn captured_at_most_once(h: &History) -> Vec<Violation> {
    let mut found = Vec::new();
    for hold in h.ledger.transfers().iter().filter(|t| t.transfer.is_pending()) {
        let posts = h.ledger.posts(hold.id());
        if posts.len() > 1 {
            found.push(Violation::new(CAPTURE_ONCE, format!("hold {} was posted {} times", hold.id(), posts.len())));
        }
        for p in posts.iter().filter(|p| p.transfer.amount > hold.transfer.amount) {
            let detail = format!("hold {} of {} was posted {}", hold.id(), hold.transfer.amount, p.transfer.amount);
            found.push(Violation::new(CAPTURE_ONCE, detail));
        }
    }
    found
}

pub fn answered_after_logged(h: &History) -> Vec<Violation> {
    let mut logged: BTreeMap<CommandId, Vec<(Decision, HistoryTime)>> = BTreeMap::new();
    for r in &h.log {
        if let Record::Outcome(c, o, _) = &r.record {
            logged.entry(c.id()).or_default().push((o.decision(), r.at));
        }
    }
    h.caller
        .iter()
        .filter_map(|r| match r {
            CallerRecord::Received(at, request, Reply::Decided(o)) => {
                let id = request.id();
                let in_time = logged
                    .get(&id)
                    .is_some_and(|ds| ds.iter().any(|(d, logged_at)| *d == o.decision() && *logged_at <= *at + h.log_clock_slack));
                (!in_time).then(|| {
                    let detail = format!("{id:?} was answered {:?} at {at}, before that decision was logged", o.decision());
                    Violation::new(LOGGED_BEFORE_ANSWERED, detail)
                })
            }
            _ => None,
        })
        .collect()
}

/// Each command's merged decision against what TigerBeetle holds. Conflicted holds are left to `no_two_outcomes`.
pub fn decisions_match_ledger(h: &History) -> Vec<Violation> {
    let mut found = Vec::new();
    for (a, hold) in holds(h) {
        let mut broken = |what: &dyn fmt::Display| found.push(Violation::new(MATCHES_LEDGER, format!("hold {a}: {what}")));
        let held = h.ledger.pending(a).is_some();
        let posts = h.ledger.posts(a);
        match hold.authorization_decision() {
            Ok(Some(AuthOutcome::Approved)) if !held => broken(&"approved, but the ledger has no hold"),
            // A key reused with a different payload is declined while the copy that holds the key keeps its hold.
            Ok(Some(AuthOutcome::Declined(reason))) if held && !matches!(reason, DeclineReason::KeyReused(_)) => {
                broken(&"declined, but the ledger holds it")
            }
            _ => {}
        }
        if let Ok(Some(CancelOutcome::CancelRefused)) = hold.cancel_decision()
            && posts.is_empty()
        {
            broken(&"cancel_refused, but the hold was never posted");
        }
        // Each capture reported, not only the merged one: a full capture must match the post, even beside a partial
        // one.
        for o in &hold.capture {
            if let CaptureOutcome::Captured(amount) = o {
                let moved = moved(*amount, a, &h.ledger);
                if !posts.iter().any(|p| moved == Some(p.transfer.amount)) {
                    broken(&format!("captured {amount:?}, but the ledger has no such post"));
                }
            }
        }
        match hold.capture_decision() {
            Ok(Some(CaptureOutcome::CaptureRefused(CaptureRefusal::Cancelled))) if h.ledger.voids(a).is_empty() => {
                broken(&"capture refused as cancelled, but the hold was never voided")
            }
            Ok(Some(CaptureOutcome::CaptureRefused(CaptureRefusal::Expired | CaptureRefusal::NotApproved))) if !posts.is_empty() => {
                broken(&"capture refused, but the hold was posted")
            }
            _ => {}
        }
    }
    found
}

/// The amount a capture moves, as the ledger records it: all of the hold when it asked for all of it.
fn moved(amount: CaptureAmount, a: TransferId, ledger: &LedgerSnapshot) -> Option<u128> {
    match amount {
        CaptureAmount::Full => ledger.pending(a).map(|t| t.transfer.amount),
        CaptureAmount::Partial(n) => Some(n.get() as u128),
    }
}

/// A key whose every authorization copy the caller sent came back refused, and was never resent after the last
/// refusal, has no hold. A hidden resend by a hop between the caller and the edge breaks it (README section 5).
pub fn refusal_leaves_no_hold(h: &History) -> Vec<Violation> {
    let mut by_key: BTreeMap<&ClientKey, Vec<&CallerRecord>> = BTreeMap::new();
    for r in h.caller.iter().filter(|r| r.request().kind() == Kind::Authorization) {
        by_key.entry(r.request().key()).or_default().push(r);
    }
    let mut found = Vec::new();
    for (key, records) in by_key {
        let sent = records.iter().filter(|r| matches!(r, CallerRecord::Sent(..))).count();
        let answers: Vec<&&CallerRecord> = records.iter().filter(|r| !matches!(r, CallerRecord::Sent(..))).collect();
        let all_refused = answers.iter().all(|r| matches!(r, CallerRecord::Received(_, _, Reply::Refused(_))));
        let a = ids::authorization(key);
        if sent > 0 && answers.len() == sent && all_refused && h.ledger.pending(a).is_some() {
            let detail = format!("every authorization for {key:?} was refused, yet the ledger holds {a}");
            found.push(Violation::new(REFUSAL_LEAVES_NO_HOLD, detail));
        }
    }
    found
}

/// A voided authorization still returns `exists`, so a caller that resends after a non-definitive result, instead
/// of cancelling, can hear `approved` about a hold the edge's own cancel released (README section 12, follow-up 7).
pub fn approval_stands(h: &History) -> Vec<Violation> {
    let cancelling: BTreeSet<TransferId> = h
        .caller
        .iter()
        .filter_map(|r| match r {
            CallerRecord::Sent(_, request @ Request::Cancel(_)) => Some(request.a()),
            _ => None,
        })
        .collect();
    let mut approved: Vec<TransferId> = Vec::new();
    for r in &h.caller {
        if let CallerRecord::Received(_, request @ Request::Authorize(_), Reply::Decided(Outcome::Auth(AuthOutcome::Approved))) = r {
            let a = request.a();
            if !approved.contains(&a) {
                approved.push(a);
            }
        }
    }
    approved
        .into_iter()
        .filter(|a| !cancelling.contains(a) && !h.ledger.voids(*a).is_empty())
        .map(|a| Violation::new(APPROVAL_STANDS, format!("hold {a} was told approved and voided, and its caller never cancelled")))
        .collect()
}

/// TigerBeetle's timestamps are on another clock than the log's, so this asks only that the command exists. A
/// transfer with no flags is left out: a fence looks like the setup's plain transfers, and the fence's own rules
/// judge it.
pub fn writes_follow_commands(h: &History) -> Vec<Violation> {
    let logged: BTreeSet<CommandId> = h
        .log
        .iter()
        .filter_map(|r: &LogRecord| match &r.record {
            Record::Command(c) => Some(c.id()),
            Record::Outcome(..) => None,
        })
        .collect();
    h.ledger
        .transfers()
        .iter()
        .filter_map(|t| {
            let needed = match t.transfer.flags.first()? {
                TransferFlag::Pending => CommandId { a: t.id(), kind: Kind::Authorization },
                TransferFlag::VoidPendingTransfer => CommandId { a: t.transfer.pending_id, kind: Kind::Cancel },
                TransferFlag::PostPendingTransfer => CommandId { a: t.transfer.pending_id, kind: Kind::Capture },
            };
            (!logged.contains(&needed)).then(|| Violation::new(WRITES_FOLLOW_COMMANDS, format!("{} has no {needed:?} in the log", t.id())))
        })
        .collect()
}

/// Replays each cluster in timestamp order, with expiries, and checks every flagged account after each change.
pub fn balances_within_limits(h: &History) -> Vec<Violation> {
    let mut limits: BTreeMap<(Period, AccountId), &LedgerAccount> = BTreeMap::new();
    for account in h.ledger.accounts() {
        limits.insert((account.cluster, account.id), account);
    }
    let mut by_cluster: BTreeMap<Period, Vec<&LedgerTransfer>> = BTreeMap::new();
    for t in h.ledger.transfers() {
        by_cluster.entry(t.cluster).or_default().push(t);
    }
    by_cluster.into_iter().flat_map(|(cluster, transfers)| replay(cluster, &transfers, &h.ledger, &limits)).collect()
}

#[derive(Clone, Copy, Default)]
struct Balance {
    debits_pending: Wide,
    debits_posted: Wide,
    credits_pending: Wide,
    credits_posted: Wide,
}

impl Balance {
    fn plus(self, d: Balance) -> Balance {
        Balance {
            debits_pending: self.debits_pending + d.debits_pending,
            debits_posted: self.debits_posted + d.debits_posted,
            credits_pending: self.credits_pending + d.credits_pending,
            credits_posted: self.credits_posted + d.credits_posted,
        }
    }
}

impl fmt::Debug for Balance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Balance({:?}, {:?}, {:?}, {:?})", self.debits_pending, self.debits_posted, self.credits_pending, self.credits_posted)
    }
}

type Deltas = Vec<(AccountId, Balance)>;

fn replay(
    cluster: Period,
    transfers: &[&LedgerTransfer],
    ledger: &LedgerSnapshot,
    limits: &BTreeMap<(Period, AccountId), &LedgerAccount>,
) -> Vec<Violation> {
    let mut holds: BTreeMap<TransferId, &LedgerTransfer> = BTreeMap::new();
    for t in transfers.iter().filter(|t| t.transfer.is_pending()) {
        holds.insert(t.id(), t);
    }
    let closed: BTreeSet<TransferId> =
        transfers.iter().filter(|t| t.transfer.is_post() || t.transfer.is_void()).map(|t| t.transfer.pending_id).collect();
    let release = |hold: &LedgerTransfer| -> Deltas {
        let amount = -Wide::from_u128(hold.transfer.amount);
        vec![
            (hold.transfer.debit, Balance { debits_pending: amount, ..Balance::default() }),
            (hold.transfer.credit, Balance { credits_pending: amount, ..Balance::default() }),
        ]
    };
    let changes = |t: &LedgerTransfer| -> Deltas {
        let amount = Wide::from_u128(t.transfer.amount);
        if t.transfer.is_pending() {
            vec![
                (t.transfer.debit, Balance { debits_pending: amount, ..Balance::default() }),
                (t.transfer.credit, Balance { credits_pending: amount, ..Balance::default() }),
            ]
        } else if t.transfer.is_void() {
            holds.get(&t.transfer.pending_id).map_or_else(Vec::new, |hold| release(hold))
        } else if t.transfer.is_post() {
            holds.get(&t.transfer.pending_id).map_or_else(Vec::new, |hold| {
                let mut deltas = release(hold);
                deltas.push((hold.transfer.debit, Balance { debits_posted: amount, ..Balance::default() }));
                deltas.push((hold.transfer.credit, Balance { credits_posted: amount, ..Balance::default() }));
                deltas
            })
        } else {
            vec![
                (t.transfer.debit, Balance { debits_posted: amount, ..Balance::default() }),
                (t.transfer.credit, Balance { credits_posted: amount, ..Balance::default() }),
            ]
        }
    };
    let expiries = holds.values().filter(|hold| !closed.contains(&hold.id())).filter_map(|hold| {
        let at = match ledger.expiries() {
            Some(seen) => seen.get(&hold.id()).copied(),
            None => hold.expires_at().filter(|at| ledger.scanned_at().get(&cluster).is_some_and(|scan| at <= scan)),
        };
        at.map(|at| (at, release(hold)))
    });
    let mut timeline: Vec<(i64, Deltas)> = transfers.iter().map(|t| (t.timestamp, changes(t))).chain(expiries).collect();
    // Stable, as Scala's `sortBy` is: changes at one timestamp keep their order.
    timeline.sort_by_key(|(at, _)| *at);

    let mut balances: BTreeMap<AccountId, Balance> = BTreeMap::new();
    let mut found = Vec::new();
    for (at, deltas) in timeline {
        let mut touched: Vec<AccountId> = Vec::new();
        for (account, delta) in &deltas {
            let next = balances.get(account).copied().unwrap_or_default().plus(*delta);
            balances.insert(*account, next);
            if !touched.contains(account) {
                touched.push(*account);
            }
        }
        for account in touched {
            let b = balances[&account];
            if let Some(limit) = limits.get(&(cluster, account)) {
                let debits = limit.debits_must_not_exceed_credits && b.debits_pending + b.debits_posted > b.credits_posted;
                let credits = limit.credits_must_not_exceed_debits && b.credits_pending + b.credits_posted > b.debits_posted;
                if debits || credits {
                    let detail = format!("account {account} in period {} at {at}: {b:?}", cluster.index());
                    found.push(Violation::new(BALANCES_WITHIN_LIMITS, detail));
                }
            }
        }
    }
    found
}
