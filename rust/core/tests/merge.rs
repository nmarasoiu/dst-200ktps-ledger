//! Port of `MergeSuite`.

mod common;

use common::*;
use ledger_core::merge::{self, Ending, Hold};
use ledger_core::*;
use proptest::prelude::*;

fn holds() -> Vec<TransferId> {
    (1..=3).map(|n| ids::authorization(&key(n))).collect()
}

fn entries() -> impl Strategy<Value = Vec<(TransferId, Outcome)>> {
    proptest::collection::vec((proptest::sample::select(holds()), gens::outcome()), 0..30)
}

proptest! {
    #[test]
    fn the_merge_does_not_depend_on_the_order_outcomes_arrive_in(
        (xs, shuffled) in entries().prop_flat_map(|xs| (Just(xs.clone()), Just(xs).prop_shuffle()))
    ) {
        prop_assert_eq!(merge::fold(shuffled), merge::fold(xs));
    }

    #[test]
    fn merging_a_duplicate_changes_nothing(xs in entries(), n in 0usize..=5) {
        let mut doubled = xs.clone();
        doubled.extend(xs.iter().take(n).copied());
        prop_assert_eq!(merge::fold(doubled), merge::fold(xs));
    }

    #[test]
    fn decisions_and_conflicts_depend_only_on_the_set_of_outcomes(
        (xs, shuffled) in entries().prop_flat_map(|xs| {
            let doubled: Vec<_> = xs.iter().chain(xs.iter()).copied().collect();
            (Just(xs), Just(doubled).prop_shuffle())
        })
    ) {
        let a = merge::fold(xs);
        let b = merge::fold(shuffled);
        prop_assert!(a.keys().eq(b.keys()));
        for (k, hold) in &a {
            prop_assert_eq!(hold.ending(), b[k].ending());
            prop_assert_eq!(hold.conflicts(), b[k].conflicts());
        }
    }
}

fn a() -> TransferId {
    holds()[0]
}

fn merged(outcomes: &[Outcome]) -> Hold {
    merge::fold(outcomes.iter().map(|o| (a(), *o))).remove(&a()).expect("the hold")
}

const APPROVED: Outcome = Outcome::Auth(AuthOutcome::Approved);
const CANCELLED: Outcome = Outcome::Cancel(CancelOutcome::Cancelled);
const CANCEL_REFUSED: Outcome = Outcome::Cancel(CancelOutcome::CancelRefused);

fn declined(r: DeclineReason) -> Outcome {
    Outcome::Auth(AuthOutcome::Declined(r))
}

fn captured(amount: CaptureAmount) -> Outcome {
    Outcome::Capture(CaptureOutcome::Captured(amount))
}

fn half() -> CaptureAmount {
    CaptureAmount::partial(50)
}

// Every pair the tables in README sections 5 and 8 can produce for one command or one hold.

#[test]
fn the_edges_approval_and_the_resolvers_fence_finding_it_approved() {
    assert_eq!(merged(&[APPROVED, APPROVED]).authorization_decision(), Ok(Some(AuthOutcome::Approved)));
}

#[test]
fn the_edges_reason_and_the_resolvers_generic_one_the_edges_reason_is_kept() {
    let specific = AuthOutcome::Declined(DeclineReason::Ledger(TbStatus::ExceedsCredits));
    assert_eq!(merged(&[Outcome::Auth(specific), declined(DeclineReason::Unknown)]).authorization_decision(), Ok(Some(specific)));
}

#[test]
fn a_declined_authorization_then_a_duplicate_after_a_cancels_fence_still_declined() {
    let rejected = AuthOutcome::Declined(DeclineReason::Ledger(TbStatus::AccountsMustBeDifferent));
    assert_eq!(merged(&[Outcome::Auth(rejected), declined(DeclineReason::NotExecuted)]).authorization_decision(), Ok(Some(rejected)));
}

#[test]
fn not_executed_from_expiry_and_from_a_cancels_fence_are_one_outcome() {
    assert!(merged(&[declined(DeclineReason::NotExecuted), declined(DeclineReason::NotExecuted)]).conflicts().is_empty());
}

#[test]
fn a_copy_declined_for_reusing_the_key_with_a_different_payload_sits_beside_the_approval() {
    let reused = AuthOutcome::Declined(DeclineReason::KeyReused(TbStatus::ExistsWithDifferentAmount));
    assert_eq!(merged(&[APPROVED, Outcome::Auth(reused)]).authorization_decision(), Ok(Some(AuthOutcome::Approved)));
    assert_eq!(merged(&[Outcome::Auth(reused)]).authorization_decision(), Ok(Some(reused)));
}

#[test]
fn approved_and_declined_for_one_authorization_is_a_conflict() {
    assert!(!merged(&[APPROVED, declined(DeclineReason::NotExecuted)]).conflicts().is_empty());
}

#[test]
fn a_cancel_keeps_the_hold_cancelled_even_when_a_duplicate_approval_lands_after_it() {
    assert_eq!(merged(&[APPROVED, CANCELLED, APPROVED]).ending(), Ok(Ending::Cancelled));
    assert_eq!(merged(&[CANCELLED, APPROVED]).ending(), Ok(Ending::Cancelled));
}

#[test]
fn the_callers_cancel_and_the_edges_own_cancel_one_outcome() {
    assert_eq!(merged(&[CANCELLED, CANCELLED]).cancel_decision(), Ok(Some(CancelOutcome::Cancelled)));
}

#[test]
fn a_cancel_that_found_the_capture_and_the_capture_captured() {
    assert_eq!(merged(&[APPROVED, CANCEL_REFUSED, captured(half())]).ending(), Ok(Ending::Captured(Some(half()))));
}

#[test]
fn a_capture_and_its_resend_captured_once() {
    assert_eq!(merged(&[captured(half()), captured(half())]).capture_decision(), Ok(Some(CaptureOutcome::Captured(half()))));
}

#[test]
fn a_capture_refused_as_exceeding_the_hold_then_a_corrected_one_captured() {
    let corrected = CaptureOutcome::Captured(CaptureAmount::partial(10));
    let refused = Outcome::Capture(CaptureOutcome::CaptureRefused(CaptureRefusal::ExceedsHold));
    assert_eq!(merged(&[refused, Outcome::Capture(corrected)]).capture_decision(), Ok(Some(corrected)));
}

#[test]
fn cancelled_and_captured_is_two_endings() {
    assert!(!merged(&[APPROVED, CANCELLED, captured(half())]).conflicts().is_empty());
}

#[test]
fn cancelled_and_cancel_refused_is_two_decisions() {
    assert!(!merged(&[CANCELLED, CANCEL_REFUSED]).conflicts().is_empty());
}

#[test]
fn a_declined_authorization_whose_hold_was_captured_is_two_endings() {
    assert!(!merged(&[declined(DeclineReason::NotExecuted), captured(half())]).conflicts().is_empty());
}

#[test]
fn captured_then_refused_as_cancelled_is_a_conflict() {
    let refused = Outcome::Capture(CaptureOutcome::CaptureRefused(CaptureRefusal::Cancelled));
    assert!(!merged(&[captured(half()), refused]).conflicts().is_empty());
}

#[test]
fn a_full_capture_beside_a_partial_of_the_whole_hold_is_one_capture_and_the_partial_says_how_much() {
    let whole = CaptureAmount::partial(100);
    let hold = merged(&[captured(CaptureAmount::Full), captured(whole)]);
    assert_eq!(hold.capture_decision(), Ok(Some(CaptureOutcome::Captured(whole))));
    assert!(hold.conflicts().is_empty());
}

#[test]
fn two_different_partial_captures_still_conflict() {
    let hold = merged(&[captured(CaptureAmount::partial(10)), captured(CaptureAmount::partial(20))]);
    assert!(hold.capture_decision().is_err());
}

#[test]
fn an_approved_hold_with_nothing_after_it_is_open() {
    assert_eq!(merged(&[APPROVED]).ending(), Ok(Ending::Open));
}
