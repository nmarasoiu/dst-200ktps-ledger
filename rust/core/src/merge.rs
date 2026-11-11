//! The one merge of outcomes every reader uses: the offline checker, the audit log and every other consumer (README
//! section 5, one merge for every reader). Port of `Merge.scala`.
//!
//! A hold's merged value is the set of every outcome seen for it, by command, so merging is set union: the result
//! does not depend on the order outcomes arrive in, and a duplicate changes nothing. The decisions are read from
//! those sets. Two different decisions for one command, or two different endings for one hold, are reported as a
//! conflict rather than merged: they break a safety invariant.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use crate::domain::{
    AuthOutcome, CancelOutcome, CaptureAmount, CaptureOutcome, CaptureRefusal, CommandId, Decision, DeclineReason, Kind, Outcome,
};
use crate::ids::TransferId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Conflict {
    TwoDecisions(CommandId, BTreeSet<Outcome>),
    TwoEndings(TransferId, BTreeSet<Outcome>),
}

/// How a hold ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ending {
    Open,
    Cancelled,
    /// The amount is unknown when the capture is known only from a cancel that found it.
    Captured(Option<CaptureAmount>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hold {
    pub a: TransferId,
    pub authorization: BTreeSet<AuthOutcome>,
    pub cancel: BTreeSet<CancelOutcome>,
    pub capture: BTreeSet<CaptureOutcome>,
}

impl Hold {
    pub fn empty(a: TransferId) -> Hold {
        Hold { a, authorization: BTreeSet::new(), cancel: BTreeSet::new(), capture: BTreeSet::new() }
    }

    pub fn of(a: TransferId, outcome: Outcome) -> Hold {
        let mut hold = Hold::empty(a);
        match outcome {
            Outcome::Auth(o) => hold.authorization.insert(o),
            Outcome::Cancel(o) => hold.cancel.insert(o),
            Outcome::Capture(o) => hold.capture.insert(o),
        };
        hold
    }

    pub fn merge(mut self, that: Hold) -> Hold {
        self.authorization.extend(that.authorization);
        self.cancel.extend(that.cancel);
        self.capture.extend(that.capture);
        self
    }

    pub fn outcomes(&self) -> BTreeSet<Outcome> {
        let auth = self.authorization.iter().copied().map(Outcome::Auth);
        let cancel = self.cancel.iter().copied().map(Outcome::Cancel);
        let capture = self.capture.iter().copied().map(Outcome::Capture);
        auth.chain(cancel).chain(capture).collect()
    }

    /// Approved, or declined with the most specific reason. A copy declined because it reused the key with a
    /// different payload can sit beside the approval of the copy that holds the key: its alarm is the signal (README
    /// section 12, follow-up 6).
    pub fn authorization_decision(&self) -> Result<Option<AuthOutcome>, Conflict> {
        let declined: Vec<DeclineReason> = self
            .authorization
            .iter()
            .filter_map(|o| match o {
                AuthOutcome::Declined(r) => Some(*r),
                AuthOutcome::Approved => None,
            })
            .collect();
        let beside_approval = declined.iter().all(|r| matches!(r, DeclineReason::KeyReused(_)));
        if self.authorization.is_empty() {
            Ok(None)
        } else if !self.authorization.contains(&AuthOutcome::Approved) {
            Ok(declined.iter().max().map(|r| AuthOutcome::Declined(*r)))
        } else if beside_approval {
            Ok(Some(AuthOutcome::Approved))
        } else {
            let outcomes = self.authorization.iter().copied().map(Outcome::Auth).collect();
            Err(Conflict::TwoDecisions(CommandId { a: self.a, kind: Kind::Authorization }, outcomes))
        }
    }

    pub fn cancel_decision(&self) -> Result<Option<CancelOutcome>, Conflict> {
        if self.cancel.len() > 1 {
            let outcomes = self.cancel.iter().copied().map(Outcome::Cancel).collect();
            Err(Conflict::TwoDecisions(CommandId { a: self.a, kind: Kind::Cancel }, outcomes))
        } else {
            Ok(self.cancel.first().copied())
        }
    }

    /// Captured, or refused with the most specific reason. A refusal TigerBeetle did not record, or one about a
    /// different amount, can sit beside a capture without contradicting it (README section 8, captures). So can a
    /// full capture beside a partial one: TigerBeetle answers `exists` only when they are the same money, a partial of
    /// the whole hold, and the partial says how much.
    pub fn capture_decision(&self) -> Result<Option<CaptureOutcome>, Conflict> {
        let mut captured = Vec::new();
        let mut spelled = Vec::new();
        let mut refused = Vec::new();
        for o in &self.capture {
            match o {
                CaptureOutcome::Captured(amount) => {
                    captured.push(*amount);
                    if matches!(amount, CaptureAmount::Partial(_)) {
                        spelled.push(*amount);
                    }
                }
                CaptureOutcome::CaptureRefused(r) => refused.push(*r),
            }
        }
        let beside_capture = refused.iter().all(|r| matches!(r, CaptureRefusal::ExceedsHold | CaptureRefusal::KeyReused(_)));
        if self.capture.is_empty() {
            Ok(None)
        } else if captured.is_empty() {
            Ok(refused.iter().max().map(|r| CaptureOutcome::CaptureRefused(*r)))
        } else if spelled.len() <= 1 && beside_capture {
            Ok(spelled.first().or(captured.first()).map(|amount| CaptureOutcome::Captured(*amount)))
        } else {
            let outcomes = self.capture.iter().copied().map(Outcome::Capture).collect();
            Err(Conflict::TwoDecisions(CommandId { a: self.a, kind: Kind::Capture }, outcomes))
        }
    }

    /// A cancel reported `cancelled` decides the ending, whatever approval came before or after it (README section
    /// 12, follow-up 7). A cancelled hold that was also captured, or a declined one that was captured, has two.
    pub fn ending(&self) -> Result<Ending, Conflict> {
        let auth = self.authorization_decision()?;
        let cancelled = self.cancel_decision()?;
        let captured = self.capture_decision()?;
        self.ending_of(auth, cancelled, captured)
    }

    /// Every conflict: those of each command, or else that of the ending.
    pub fn conflicts(&self) -> Vec<Conflict> {
        let of_commands: Vec<Conflict> = [self.authorization_decision().err(), self.cancel_decision().err(), self.capture_decision().err()]
            .into_iter()
            .flatten()
            .collect();
        if !of_commands.is_empty() { of_commands } else { self.ending().err().into_iter().collect() }
    }

    fn ending_of(
        &self,
        auth: Option<AuthOutcome>,
        cancelled: Option<CancelOutcome>,
        captured: Option<CaptureOutcome>,
    ) -> Result<Ending, Conflict> {
        let captured_amount = match captured {
            Some(CaptureOutcome::Captured(amount)) => Some(amount),
            _ => None,
        };
        let is_cancelled = cancelled == Some(CancelOutcome::Cancelled);
        let is_captured = captured_amount.is_some() || cancelled == Some(CancelOutcome::CancelRefused);
        let is_declined = auth.is_some_and(|o| Outcome::Auth(o).decision() == Decision::Declined);
        if (is_cancelled || is_declined) && is_captured {
            Err(Conflict::TwoEndings(self.a, self.outcomes()))
        } else if is_cancelled {
            Ok(Ending::Cancelled)
        } else if is_captured {
            Ok(Ending::Captured(captured_amount))
        } else {
            Ok(Ending::Open)
        }
    }
}

/// Every outcome, keyed by the hold `A` it concerns.
pub fn fold(outcomes: impl IntoIterator<Item = (TransferId, Outcome)>) -> BTreeMap<TransferId, Hold> {
    let mut acc: BTreeMap<TransferId, Hold> = BTreeMap::new();
    for (a, outcome) in outcomes {
        let one = Hold::of(a, outcome);
        let merged = match acc.remove(&a) {
            Some(hold) => hold.merge(one),
            None => one,
        };
        acc.insert(a, merged);
    }
    acc
}
