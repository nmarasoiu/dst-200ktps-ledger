//! Port of `CallerSuite`.

mod common;

use common::*;
use ledger_core::caller::{self, Effect, Event, Phase};
use ledger_core::*;
use proptest::prelude::*;

type Steps = Vec<Step<caller::State, Effect>>;

fn k() -> ClientKey {
    key(1)
}

fn auth() -> Request {
    authorize(&k())
}

fn run(events: &[Event]) -> Steps {
    let mut steps = vec![caller::start(authorize_request(&k()))];
    for e in events {
        let last = steps.last().expect("a step").state.clone();
        steps.push(caller::step(last, *e));
    }
    steps
}

fn sent(steps: &Steps) -> Vec<Request> {
    steps.iter().flat_map(|s| &s.effects).map(|Effect::Send(r)| r.clone()).collect()
}

const CANCELLED: Reply = Reply::Decided(Outcome::Cancel(CancelOutcome::Cancelled));
const APPROVED: Reply = Reply::Decided(Outcome::Auth(AuthOutcome::Approved));

#[test]
fn section_8_a_timed_out_authorization_is_cancelled_and_the_cancel_resent_until_it_is_answered() {
    let steps = run(&[Event::Replied(Reply::TimedOut), Event::Lost, Event::Replied(Reply::Retry), Event::Replied(CANCELLED)]);
    let cancel = Request::Cancel(k());
    assert_eq!(sent(&steps), vec![auth(), cancel.clone(), cancel.clone(), cancel]);
    assert_eq!(steps.last().unwrap().state.phase, Phase::Done);
    assert_eq!(steps[1].marks, vec![Mark::CallerCancelsAfterNonDefinitive]);
    assert_eq!(steps[2].marks, vec![Mark::CallerResendsCancel]);
}

#[test]
fn section_5_a_lost_reply_is_a_timeout_from_the_callers_view_so_the_caller_cancels() {
    assert_eq!(sent(&run(&[Event::Lost])), vec![auth(), Request::Cancel(k())]);
}

#[test]
fn section_5_a_refusal_may_be_resent_with_the_same_key_and_needs_no_cancel() {
    let steps = run(&[Event::Replied(Reply::Refused(RefusalReason::Overload)), Event::Resend, Event::Replied(APPROVED)]);
    assert_eq!(sent(&steps), vec![auth(), auth()]);
    assert_eq!(steps.last().unwrap().state.phase, Phase::Approved);
    assert_eq!(steps[2].marks, vec![Mark::CallerResendsRefused]);
}

#[test]
fn section_8_a_capture_follows_an_approval_and_is_resent_with_the_same_key_until_answered() {
    let half = CaptureAmount::partial(50);
    let steps = run(&[Event::Replied(APPROVED), Event::Capture(half), Event::Replied(Reply::Retry), Event::Lost]);
    let capture = Request::Capture(k(), half);
    assert_eq!(sent(&steps), vec![auth(), capture.clone(), capture.clone(), capture]);
    assert_eq!(steps.last().unwrap().marks, vec![Mark::CallerResendsCapture]);
}

#[test]
fn section_5_a_hold_is_abandoned_an_approved_caller_stops_and_tigerbeetles_timeout_is_what_ends_the_hold() {
    let steps = run(&[Event::Replied(APPROVED), Event::Stop]);
    assert_eq!(sent(&steps), vec![auth()]);
    assert_eq!(steps.last().unwrap().state.phase, Phase::Done);
    assert_eq!(steps.last().unwrap().marks, vec![Mark::CallerAbandonedHold]);
}

#[test]
fn section_5_a_capture_can_only_follow_an_approval() {
    let steps = run(&[Event::Capture(CaptureAmount::Full)]);
    assert_eq!(sent(&steps), vec![auth()]);
    assert_eq!(steps.last().unwrap().marks, vec![Mark::CallerIgnoredOutOfTurn]);
}

fn events() -> impl Strategy<Value = Vec<Event>> {
    proptest::collection::vec(
        prop_oneof![
            Just(Event::Lost),
            Just(Event::Resend),
            Just(Event::Stop),
            gens::capture_amount().prop_map(Event::Capture),
            prop_oneof![Just(Reply::TimedOut), Just(Reply::Retry), Just(Reply::Refused(RefusalReason::Overload))].prop_map(Event::Replied),
            gens::outcome().prop_map(|o| Event::Replied(Reply::Decided(o))),
        ],
        0..20,
    )
}

proptest! {
    #[test]
    fn the_caller_never_resends_an_authorization_after_a_non_definitive_result(es in events()) {
        let steps = run(&es);
        if let Some(at) = steps.iter().position(|s| s.marks.contains(&Mark::CallerCancelsAfterNonDefinitive)) {
            prop_assert!(!sent(&steps[at..].to_vec()).contains(&auth()));
        }
    }

    #[test]
    fn the_caller_cancels_only_after_a_non_definitive_result_and_captures_only_after_an_approval(es in events()) {
        let steps = run(&es);
        let phases: Vec<Phase> = steps.iter().map(|s| s.state.phase).collect();
        for (i, s) in steps.iter().enumerate() {
            for Effect::Send(r) in &s.effects {
                match r {
                    Request::Cancel(_) => prop_assert!(phases[..=i].contains(&Phase::Cancelling)),
                    Request::Capture(..) => prop_assert!(phases[..i].contains(&Phase::Approved)),
                    Request::Authorize(_) => {}
                }
            }
        }
    }
}
