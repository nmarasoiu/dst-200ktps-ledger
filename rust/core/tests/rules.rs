//! Port of `RulesSuite`: one example per row of the tables in README sections 5 and 8, each asserting its coverage
//! mark.

mod common;

use std::collections::BTreeSet;

use common::*;
use ledger_core::TbStatus::*;
use ledger_core::rules::{self, Verdict};
use ledger_core::tb::transfers;
use ledger_core::*;

const APPROVED: AuthOutcome = AuthOutcome::Approved;
const NOT_EXECUTED: AuthOutcome = AuthOutcome::Declined(DeclineReason::NotExecuted);
const UNKNOWN: AuthOutcome = AuthOutcome::Declined(DeclineReason::Unknown);

#[track_caller]
fn assert_decided<O: PartialEq + std::fmt::Debug>(v: Verdict<O>, outcome: O, mark: Mark, alarm: bool) {
    match v {
        Verdict::Decided(o, m, a) => {
            assert_eq!(o, outcome);
            assert_eq!(m, mark);
            assert_eq!(a.is_some(), alarm, "alarm: {a:?}");
        }
        other => panic!("expected {outcome:?}, got {other:?}"),
    }
}

#[track_caller]
fn assert_unresolved<O: std::fmt::Debug>(v: Verdict<O>, mark: Mark) {
    match v {
        Verdict::Unresolved(m, _) => assert_eq!(m, mark),
        other => panic!("expected no outcome, got {other:?}"),
    }
}

// README section 5: the authorization's own result.

#[test]
fn section_5_the_edges_authorization_is_created_approved() {
    assert_decided(rules::authorization(Created), APPROVED, Mark::AuthorizationCreated, false);
}

#[test]
fn section_5_the_same_client_key_twice_the_original_approval_comes_back_as_exists() {
    assert_decided(rules::authorization(Exists), APPROVED, Mark::AuthorizationExists, false);
}

#[test]
fn section_5_a_paused_edges_authorization_after_the_resolvers_fence_not_executed() {
    assert_decided(rules::authorization(ExistsWithDifferentFlags), NOT_EXECUTED, Mark::AuthorizationFenced, false);
}

#[test]
fn section_5_a_refused_authorization_resent_after_an_earlier_copy_failed_declined_reason_unknown() {
    assert_decided(rules::authorization(IdAlreadyFailed), UNKNOWN, Mark::AuthorizationIdAlreadyFailed, false);
}

#[test]
fn section_5_a_transient_failure_is_declined_with_tigerbeetles_reason() {
    let declined = AuthOutcome::Declined(DeclineReason::Ledger(ExceedsCredits));
    assert_decided(rules::authorization(ExceedsCredits), declined, Mark::AuthorizationDeclined, false);
}

#[test]
fn section_12_follow_up_6_a_key_reused_with_a_different_amount_is_told_apart_from_a_fence_by_its_code() {
    let declined = AuthOutcome::Declined(DeclineReason::KeyReused(ExistsWithDifferentAmount));
    assert_decided(rules::authorization(ExistsWithDifferentAmount), declined, Mark::AuthorizationKeyReused, true);
}

#[test]
fn a_validation_failure_is_declined_and_not_recorded() {
    let declined = AuthOutcome::Declined(DeclineReason::Ledger(AccountsMustBeDifferent));
    assert_decided(rules::authorization(AccountsMustBeDifferent), declined, Mark::AuthorizationRejected, false);
}

// README section 5 and section 8, resolver: the fence written for a straggler.

#[test]
fn section_5_the_edge_crashed_after_calling_the_ledger_the_fence_finds_the_authorization_approved() {
    assert_decided(rules::expiry(ExistsWithDifferentFlags), APPROVED, Mark::ExpiryFoundAuthorization, false);
}

#[test]
fn section_5_the_edge_crashed_after_a_declined_call_the_fence_finds_the_burned_id_declined() {
    assert_decided(rules::expiry(IdAlreadyFailed), UNKNOWN, Mark::ExpiryFoundDeclined, false);
}

#[test]
fn section_5_the_edge_crashed_before_calling_the_ledger_the_fence_takes_a_not_executed() {
    assert_decided(rules::expiry(Created), NOT_EXECUTED, Mark::ExpiryFenced, false);
}

#[test]
fn section_5_a_zombie_resolvers_fence_meets_the_first_fence_not_executed_again() {
    assert_decided(rules::expiry(Exists), NOT_EXECUTED, Mark::ExpiryFoundFence, false);
}

#[test]
fn a_fence_that_fails_transiently_still_burns_a_not_executed_with_an_alarm() {
    assert_decided(rules::expiry(DebitAccountNotFound), NOT_EXECUTED, Mark::ExpiryFenceBurned, true);
}

#[test]
fn a_fence_that_meets_a_fence_with_different_fields_not_executed_with_an_alarm() {
    assert_decided(rules::expiry(ExistsWithDifferentCode), NOT_EXECUTED, Mark::ExpiryFenceChanged, true);
}

#[test]
fn a_fence_rejected_by_validation_was_not_recorded_so_a_is_not_fenced_no_outcome() {
    assert_unresolved(rules::expiry(LedgerMustNotBeZero), Mark::ExpiryUnresolved);
}

// README section 8: the fence table, and its replay.

#[test]
fn fence_table() {
    use CancelOutcome::{CancelRefused, Cancelled};
    // The authorization first; the void releases the hold.
    assert_decided(rules::cancel(ExistsWithDifferentFlags, Created), Cancelled, Mark::CancelReleasedHold, false);
    // The authorization first, already timed out.
    assert_decided(rules::cancel(ExistsWithDifferentFlags, PendingTransferExpired), Cancelled, Mark::CancelFoundHoldExpired, false);
    // The authorization failed transiently; nothing was held.
    assert_decided(rules::cancel(IdAlreadyFailed, PendingTransferNotFound), Cancelled, Mark::CancelNothingHeld, false);
    // The cancel first; the fence takes A and the void fails harmlessly.
    assert_decided(rules::cancel(Created, PendingTransferNotPending), Cancelled, Mark::CancelFencedFirst, false);
    // The authorization, then its capture: cancel_refused.
    assert_decided(rules::cancel(ExistsWithDifferentFlags, PendingTransferAlreadyPosted), CancelRefused, Mark::CancelFoundCapture, false);
    // A cancel's fence meets the resolver's fence: cancelled.
    assert_decided(rules::cancel(Exists, PendingTransferNotPending), Cancelled, Mark::CancelFoundFence, false);
}

#[test]
fn fence_table_replays() {
    use CancelOutcome::Cancelled;
    // A released hold's void returns exists, and the outcome repeats.
    assert_decided(rules::cancel(ExistsWithDifferentFlags, Exists), Cancelled, Mark::CancelReplayedVoid, false);
    // A void that burned V returns id_already_failed, and the outcome repeats.
    assert_decided(rules::cancel(IdAlreadyFailed, IdAlreadyFailed), Cancelled, Mark::CancelReplayedNothingHeld, false);
    // A fence created by the first try returns exists.
    assert_decided(rules::cancel(Exists, PendingTransferNotPending), Cancelled, Mark::CancelFoundFence, false);
}

#[test]
fn a_fence_that_fails_transiently_burns_a_cancelled_with_an_alarm() {
    let cancelled = CancelOutcome::Cancelled;
    assert_decided(rules::cancel(CreditAccountAlreadyClosed, PendingTransferNotFound), cancelled, Mark::CancelFenceBurned, true);
    assert_decided(rules::cancel(CreditAccountAlreadyClosed, IdAlreadyFailed), cancelled, Mark::CancelFenceBurned, true);
}

#[test]
fn a_fence_with_changed_fields_still_holds_a_cancelled_with_an_alarm() {
    let cancelled = CancelOutcome::Cancelled;
    assert_decided(rules::cancel(ExistsWithDifferentLedger, PendingTransferNotPending), cancelled, Mark::CancelFenceChanged, true);
}

#[test]
fn a_fence_rejected_by_validation_leaves_a_unfenced_never_reported_cancelled() {
    assert_unresolved(rules::cancel(LedgerMustNotBeZero, PendingTransferNotFound), Mark::CancelUnresolved);
}

#[test]
fn a_combination_no_row_lists_is_never_reported_cancelled() {
    assert_unresolved(rules::cancel(Created, Created), Mark::CancelUnresolved);
    assert_unresolved(rules::cancel(ExistsWithDifferentFlags, PendingTransferAlreadyVoided), Mark::CancelUnresolved);
}

// README section 8: the capture table.

fn half() -> CaptureAmount {
    CaptureAmount::partial(50)
}

#[test]
fn capture_table() {
    use CaptureOutcome::{CaptureRefused, Captured};
    // A live hold: captured, with the amount.
    assert_decided(rules::capture(Created, half()), Captured(half()), Mark::CaptureCreated, false);
    // Its own earlier capture, resent: captured with the same amount.
    assert_decided(rules::capture(Exists, CaptureAmount::Full), Captured(CaptureAmount::Full), Mark::CaptureReplayed, false);
    // Its own earlier capture with a different amount.
    let reused = CaptureRefused(CaptureRefusal::KeyReused(ExistsWithDifferentAmount));
    assert_decided(rules::capture(ExistsWithDifferentAmount, half()), reused, Mark::CaptureKeyReused, true);
    // A hold voided by a cancel.
    assert_decided(
        rules::capture(PendingTransferAlreadyVoided, half()),
        CaptureRefused(CaptureRefusal::Cancelled),
        Mark::CaptureFoundVoid,
        false,
    );
    // A hold past its timeout.
    assert_decided(
        rules::capture(PendingTransferExpired, half()),
        CaptureRefused(CaptureRefusal::Expired),
        Mark::CaptureFoundExpired,
        false,
    );
    // A hold smaller than the amount.
    let exceeds = CaptureRefused(CaptureRefusal::ExceedsHold);
    assert_decided(rules::capture(ExceedsPendingTransferAmount, half()), exceeds, Mark::CaptureExceedsHold, false);
    // A fence.
    let not_approved = CaptureRefused(CaptureRefusal::NotApproved);
    assert_decided(rules::capture(PendingTransferNotPending, half()), not_approved, Mark::CaptureFoundFence, false);
    // No hold at all, then its resend.
    assert_decided(rules::capture(PendingTransferNotFound, half()), not_approved, Mark::CaptureFoundNoHold, false);
    assert_decided(rules::capture(IdAlreadyFailed, half()), not_approved, Mark::CaptureReplayedNoHold, false);
}

#[test]
fn a_post_that_cannot_happen_gives_no_outcome() {
    assert_unresolved(rules::capture(PendingTransferAlreadyPosted, half()), Mark::CaptureUnresolved);
}

#[test]
fn another_rejection_by_tigerbeetle_a_closed_account_refuses_the_capture() {
    let refused = CaptureOutcome::CaptureRefused(CaptureRefusal::Ledger(DebitAccountAlreadyClosed));
    assert_decided(rules::capture(DebitAccountAlreadyClosed, half()), refused, Mark::CaptureRejected, false);
}

#[test]
fn results_that_do_not_fit_the_command_give_no_outcome() {
    assert_unresolved(rules::decide(&auth_command(&key(1)), false, &[]), Mark::AuthorizationUnresolved);
}

// The transfers each command becomes.

#[test]
fn an_authorization_is_a_pending_transfer_with_id_a_and_the_stamped_hold_timeout() {
    let c = authorize_command(&key(1));
    let t = transfers::authorization(&c, &LEDGER_CFG);
    assert_eq!(t.id, ids::authorization(&c.key));
    assert_eq!(t.flags, TransferFlags::of(TransferFlag::Pending));
    assert_eq!(t.timeout_seconds, HOLD_TIMEOUT);
}

#[test]
fn a_cancel_is_the_fence_then_the_void_with_amount_0_and_fields_inherited() {
    let a = ids::authorization(&key(1));
    let [fence, void] = transfers::cancel(a, &LEDGER_CFG).try_into().expect("two transfers");
    assert_eq!((fence.id, fence.amount, fence.flags, fence.pending_id), (a, 0, TransferFlags::NONE, TransferId::ZERO));
    assert_eq!((void.id, void.pending_id, void.amount, void.ledger, void.code), (ids::void(a), a, 0, 0, 0));
}

#[test]
fn every_fence_is_identical_whoever_writes_it() {
    let c = auth_command(&key(1));
    assert_eq!(transfers::for_resolver(&c, &LEDGER_CFG), transfers::for_edge(&c, true, &LEDGER_CFG));
    assert_eq!(transfers::for_resolver(&c, &LEDGER_CFG)[0], transfers::cancel(c.a(), &LEDGER_CFG)[0]);
}

#[test]
fn a_full_capture_is_sent_as_the_maximum_amount_a_partial_one_as_its_amount_never_a_raw_0() {
    let a = ids::authorization(&key(1));
    assert_eq!(transfers::post(a, CaptureAmount::Full).amount, u128::MAX);
    assert_eq!(transfers::post(a, half()).amount, 50);
    assert!(CaptureAmount::try_partial(0).is_err());
}

#[test]
fn every_mark_of_the_rules_is_reachable_from_some_result() {
    let mut reached = BTreeSet::new();
    let mut mark_of = |v: Verdict<()>| {
        reached.insert(match v {
            Verdict::Decided(_, m, _) | Verdict::Unresolved(m, _) => m,
        });
    };
    for s in TbStatus::ALL {
        mark_of(rules::authorization(*s).map(|_| ()));
        mark_of(rules::expiry(*s).map(|_| ()));
        mark_of(rules::capture(*s, half()).map(|_| ()));
        for v in TbStatus::ALL {
            mark_of(rules::cancel(*s, *v).map(|_| ()));
        }
    }
    let rule_marks: BTreeSet<Mark> = Mark::ALL
        .iter()
        .copied()
        .filter(|m| ["Authorization", "Expiry", "Cancel", "Capture"].iter().any(|p| m.name().starts_with(p)))
        .filter(|m| *m != Mark::AuthorizationUnresolved)
        .collect();
    assert_eq!(rule_marks.difference(&reached).collect::<Vec<_>>(), Vec::<&Mark>::new());
}

#[test]
fn the_numeric_codes_match_tigerbeetles_source_for_the_codes_the_rules_name() {
    assert_eq!(Created.code(), 0xffff_ffff);
    assert_eq!(ExistsWithDifferentFlags.code(), 36);
    assert_eq!(Exists.code(), 46);
    assert_eq!(IdAlreadyFailed.code(), 68);
    assert_eq!(PendingTransferNotFound.code(), 25);
    assert_eq!(PendingTransferNotPending.code(), 26);
    assert_eq!(PendingTransferExpired.code(), 35);
    let codes: BTreeSet<u32> = TbStatus::ALL.iter().map(|s| s.code()).collect();
    assert_eq!(codes.len(), TbStatus::ALL.len());
}
