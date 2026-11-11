//! The rules: each maps TigerBeetle's results for one kind of command to an outcome, one case per table row (README
//! sections 5 and 8). The edge and the resolver use the same rules (README section 2, principle 5). Port of
//! `Rules.scala`.

use alloc::vec;

use crate::domain::{
    Alarm, AuthOutcome, CancelOutcome, CaptureAmount, CaptureOutcome, CaptureRefusal, Command, DeclineReason, Kind, Outcome,
};
use crate::mark::Mark;
use crate::tb::{TbGroup as G, TbStatus};

/// What a rule makes of TigerBeetle's results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict<O> {
    Decided(O, Mark, Option<Alarm>),
    /// No definitive outcome: nothing safe can be said. The edge answers as on a timeout, and the resolver leaves the
    /// command without an outcome and tries again later.
    Unresolved(Mark, Alarm),
}

impl<O> Verdict<O> {
    pub fn map<P>(self, f: impl FnOnce(O) -> P) -> Verdict<P> {
        match self {
            Verdict::Decided(o, mark, alarm) => Verdict::Decided(f(o), mark, alarm),
            Verdict::Unresolved(mark, alarm) => Verdict::Unresolved(mark, alarm),
        }
    }
}

fn decided<O>(outcome: O, mark: Mark) -> Verdict<O> {
    Verdict::Decided(outcome, mark, None)
}

fn alarmed<O>(outcome: O, mark: Mark, alarm: Alarm) -> Verdict<O> {
    Verdict::Decided(outcome, mark, Some(alarm))
}

fn unexpected(kind: Kind, status: TbStatus) -> Alarm {
    Alarm::UnexpectedResults(kind, vec![status])
}

const NOT_EXECUTED: AuthOutcome = AuthOutcome::Declined(DeclineReason::NotExecuted);
const UNKNOWN: AuthOutcome = AuthOutcome::Declined(DeclineReason::Unknown);

/// An authorization the edge submitted as a pending transfer with ID `A`.
pub fn authorization(status: TbStatus) -> Verdict<AuthOutcome> {
    use AuthOutcome::{Approved, Declined};
    match status.group() {
        G::Created => decided(Approved, Mark::AuthorizationCreated),
        G::Exists => decided(Approved, Mark::AuthorizationExists),
        G::ExistsWithDifferentFlags => decided(NOT_EXECUTED, Mark::AuthorizationFenced),
        G::ExistsWithDifferentField(s) => alarmed(Declined(DeclineReason::KeyReused(s)), Mark::AuthorizationKeyReused, Alarm::KeyReused(s)),
        G::IdAlreadyFailed => decided(UNKNOWN, Mark::AuthorizationIdAlreadyFailed),
        G::Transient(s) => decided(Declined(DeclineReason::Ledger(s)), Mark::AuthorizationDeclined),
        G::Rejected(s) => decided(Declined(DeclineReason::Ledger(s)), Mark::AuthorizationRejected),
        // Codes of a post or a void, which a pending transfer never gets. The hold was not created, so the decline is
        // true; the alarm flags the impossible result.
        G::PendingNotPending | G::AlreadyPosted | G::AlreadyVoided | G::PendingExpired | G::ExceedsPendingAmount => {
            alarmed(Declined(DeclineReason::Ledger(status)), Mark::AuthorizationRejected, unexpected(Kind::Authorization, status))
        }
    }
}

/// The fence for `A`, written instead of an authorization: by the resolver for a straggler, or by the edge when the
/// authorization's period has ended. The ledger decides, not a clock (README section 8, resolver).
pub fn expiry(fence: TbStatus) -> Verdict<AuthOutcome> {
    match fence.group() {
        G::ExistsWithDifferentFlags => decided(AuthOutcome::Approved, Mark::ExpiryFoundAuthorization),
        G::IdAlreadyFailed => decided(UNKNOWN, Mark::ExpiryFoundDeclined),
        G::Created => decided(NOT_EXECUTED, Mark::ExpiryFenced),
        G::Exists => decided(NOT_EXECUTED, Mark::ExpiryFoundFence),
        G::Transient(s) => alarmed(NOT_EXECUTED, Mark::ExpiryFenceBurned, Alarm::FenceBurned(s)),
        G::ExistsWithDifferentField(s) => alarmed(NOT_EXECUTED, Mark::ExpiryFenceChanged, Alarm::FenceChanged(s)),
        // A rejected fence was not recorded, so `A` is not fenced and a late authorization could still create it.
        G::Rejected(_) | G::PendingNotPending | G::AlreadyPosted | G::AlreadyVoided | G::PendingExpired | G::ExceedsPendingAmount => {
            Verdict::Unresolved(Mark::ExpiryUnresolved, unexpected(Kind::Authorization, fence))
        }
    }
}

/// A cancel: the fence `A` and the void `V`, in one batch, fence first (README section 8, the fence table). Only rows
/// that guarantee no hold survives report `cancelled`.
pub fn cancel(fence: TbStatus, void: TbStatus) -> Verdict<CancelOutcome> {
    use CancelOutcome::{CancelRefused, Cancelled};
    match (fence.group(), void.group()) {
        (G::ExistsWithDifferentFlags, G::Created) => decided(Cancelled, Mark::CancelReleasedHold),
        (G::ExistsWithDifferentFlags, G::Exists) => decided(Cancelled, Mark::CancelReplayedVoid),
        (G::ExistsWithDifferentFlags, G::PendingExpired) => decided(Cancelled, Mark::CancelFoundHoldExpired),
        (G::ExistsWithDifferentFlags, G::AlreadyPosted) => decided(CancelRefused, Mark::CancelFoundCapture),
        (G::IdAlreadyFailed, G::Transient(TbStatus::PendingTransferNotFound)) => decided(Cancelled, Mark::CancelNothingHeld),
        (G::IdAlreadyFailed, G::IdAlreadyFailed) => decided(Cancelled, Mark::CancelReplayedNothingHeld),
        (G::Created, G::PendingNotPending) => decided(Cancelled, Mark::CancelFencedFirst),
        (G::Exists, G::PendingNotPending) => decided(Cancelled, Mark::CancelFoundFence),
        // The ID check runs first, so a transient failure means `A` did not exist; it is burned now.
        (G::Transient(s), G::Transient(TbStatus::PendingTransferNotFound) | G::IdAlreadyFailed) => {
            alarmed(Cancelled, Mark::CancelFenceBurned, Alarm::FenceBurned(s))
        }
        (G::ExistsWithDifferentField(s), G::PendingNotPending) => alarmed(Cancelled, Mark::CancelFenceChanged, Alarm::FenceChanged(s)),
        _ => Verdict::Unresolved(Mark::CancelUnresolved, Alarm::UnexpectedResults(Kind::Cancel, vec![fence, void])),
    }
}

/// A capture: the post `P` of the hold `A` (README section 8, the capture table). Never fenced or expired.
pub fn capture(post: TbStatus, amount: CaptureAmount) -> Verdict<CaptureOutcome> {
    use CaptureOutcome::{CaptureRefused, Captured};
    match post.group() {
        G::Created => decided(Captured(amount), Mark::CaptureCreated),
        G::Exists => decided(Captured(amount), Mark::CaptureReplayed),
        G::ExistsWithDifferentField(s) => {
            alarmed(CaptureRefused(CaptureRefusal::KeyReused(s)), Mark::CaptureKeyReused, Alarm::KeyReused(s))
        }
        G::AlreadyVoided => decided(CaptureRefused(CaptureRefusal::Cancelled), Mark::CaptureFoundVoid),
        G::PendingExpired => decided(CaptureRefused(CaptureRefusal::Expired), Mark::CaptureFoundExpired),
        G::ExceedsPendingAmount => decided(CaptureRefused(CaptureRefusal::ExceedsHold), Mark::CaptureExceedsHold),
        G::PendingNotPending => decided(CaptureRefused(CaptureRefusal::NotApproved), Mark::CaptureFoundFence),
        G::Transient(TbStatus::PendingTransferNotFound) => decided(CaptureRefused(CaptureRefusal::NotApproved), Mark::CaptureFoundNoHold),
        G::IdAlreadyFailed => decided(CaptureRefused(CaptureRefusal::NotApproved), Mark::CaptureReplayedNoHold),
        G::Transient(s) => decided(CaptureRefused(CaptureRefusal::Ledger(s)), Mark::CaptureRejected),
        G::Rejected(s) => decided(CaptureRefused(CaptureRefusal::Ledger(s)), Mark::CaptureRejected),
        // `P` is only ever a post, and only `P` posts the hold, so neither can happen.
        G::ExistsWithDifferentFlags | G::AlreadyPosted => Verdict::Unresolved(Mark::CaptureUnresolved, unexpected(Kind::Capture, post)),
    }
}

/// The results of one ledger call for a command. `fence_path` is true when the authorization was fenced rather than
/// submitted: always for the resolver, and for the edge once the period has ended.
pub fn decide(command: &Command, fence_path: bool, results: &[TbStatus]) -> Verdict<Outcome> {
    match (command, results) {
        (Command::Authorize(_), [r]) => {
            if fence_path {
                expiry(*r).map(Outcome::Auth)
            } else {
                authorization(*r).map(Outcome::Auth)
            }
        }
        (Command::Cancel(_), [fence, void]) => cancel(*fence, *void).map(Outcome::Cancel),
        (Command::Capture(_, amount), [post]) => capture(*post, *amount).map(Outcome::Capture),
        (c, _) => {
            let mark = match c.kind() {
                Kind::Authorization if fence_path => Mark::ExpiryUnresolved,
                Kind::Authorization => Mark::AuthorizationUnresolved,
                Kind::Cancel => Mark::CancelUnresolved,
                Kind::Capture => Mark::CaptureUnresolved,
            };
            Verdict::Unresolved(mark, Alarm::UnexpectedResults(c.kind(), results.to_vec()))
        }
    }
}
