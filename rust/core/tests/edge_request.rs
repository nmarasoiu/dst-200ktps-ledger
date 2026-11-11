//! Port of `EdgeRequestSuite`.

mod common;

use std::collections::BTreeSet;

use common::*;
use ledger_core::edge_request::{self, Effect, Event, Origin, Phase};
use ledger_core::tb::transfers;
use ledger_core::*;
use proptest::prelude::*;

const START: EdgeTime = EdgeTime::new(0);

/// The steps of one request: the time, the event (None for the start) and the effects and marks.
type Trace = Vec<(EdgeTime, Option<Event>, Step<edge_request::State, Effect>)>;

fn run_with(request: Request, events: &[(i64, Event)], origin: Origin, wall: i64, cfg: &edge_request::Config) -> Trace {
    let first = edge_request::start(request, origin, START, WallTime::new(wall), cfg);
    let mut trace: Trace = vec![(START, None, first)];
    for (at, e) in events {
        let state = trace.last().expect("a step").2.state.clone();
        let step = edge_request::step(state, EdgeTime::new(*at), e.clone(), cfg);
        trace.push((EdgeTime::new(*at), Some(e.clone()), step));
    }
    trace
}

fn run(request: Request, events: &[(i64, Event)]) -> Trace {
    run_with(request, events, Origin::Caller, T0, &edge_cfg())
}

fn effects(t: &Trace) -> Vec<Effect> {
    t.iter().flat_map(|(_, _, s)| s.effects.clone()).collect()
}

fn marks(t: &Trace) -> Vec<Mark> {
    t.iter().flat_map(|(_, _, s)| s.marks.clone()).collect()
}

fn answers(t: &Trace) -> Vec<Reply> {
    effects(t)
        .into_iter()
        .filter_map(|e| match e {
            Effect::Answer(r) => Some(r),
            _ => None,
        })
        .collect()
}

fn submits(t: &Trace) -> Vec<Vec<Transfer>> {
    effects(t)
        .into_iter()
        .filter_map(|e| match e {
            Effect::Submit(_, ts) => Some(ts),
            _ => None,
        })
        .collect()
}

fn k() -> ClientKey {
    key(1)
}

fn returned(statuses: &[TbStatus]) -> Event {
    Event::LedgerReturned(statuses.to_vec())
}

const APPROVED: Outcome = Outcome::Auth(AuthOutcome::Approved);

// Example tests: the rows of README sections 4, 5 and 8 that the edge's flow handles.

#[test]
fn an_authorization_log_call_the_ledger_log_the_outcome_then_answer() {
    let t =
        run(authorize(&k()), &[(10, Event::CommandAcknowledged), (20, returned(&[TbStatus::Created])), (30, Event::OutcomeAcknowledged)]);
    let without_wakes: Vec<Effect> = effects(&t).into_iter().filter(|e| !matches!(e, Effect::WakeAt(_))).collect();
    assert_eq!(
        without_wakes,
        vec![
            Effect::AppendCommand(auth_command(&k())),
            Effect::Submit(Period::new(0), vec![transfers::authorization(&authorize_command(&k()), &LEDGER_CFG)]),
            Effect::AppendOutcome(auth_command(&k()), APPROVED),
            Effect::Answer(Reply::Decided(APPROVED)),
        ]
    );
    assert_eq!(marks(&t), vec![Mark::AuthorizationCreated, Mark::EdgeAnswered]);
}

#[test]
fn section_5_the_command_log_does_not_acknowledge_the_command_refused_and_the_ledger_is_never_called() {
    let t = run(authorize(&k()), &[(10, Event::CommandFailed)]);
    assert_eq!(answers(&t), vec![Reply::Refused(RefusalReason::CommandNotAcknowledged)]);
    assert!(submits(&t).is_empty());
    assert!(marks(&t).contains(&Mark::EdgeRefusedCommandNotAcknowledged));
}

#[test]
fn section_5_the_command_is_acknowledged_too_late_for_the_ledger_call_and_the_outcome_write_refused() {
    let t = run(authorize(&k()), &[(1250, Event::CommandAcknowledged)]);
    assert_eq!(answers(&t), vec![Reply::Refused(RefusalReason::AcknowledgedTooLate)]);
    assert!(submits(&t).is_empty());
    assert!(marks(&t).contains(&Mark::EdgeRefusedAcknowledgedTooLate));
}

#[test]
fn section_5_no_acknowledgement_by_the_hand_off_budget_refused_not_timed_out() {
    let t = run(authorize(&k()), &[(1200, Event::TimeAdvanced), (1300, Event::CommandAcknowledged)]);
    assert_eq!(answers(&t), vec![Reply::Refused(RefusalReason::AcknowledgedTooLate)]);
    assert!(submits(&t).is_empty());
}

#[test]
fn section_5_the_outcome_write_fails_answered_as_on_a_timeout_and_the_edge_cancels() {
    let t = run(authorize(&k()), &[(10, Event::CommandAcknowledged), (20, returned(&[TbStatus::Created])), (30, Event::OutcomeFailed)]);
    assert_eq!(answers(&t), vec![Reply::TimedOut]);
    assert!(effects(&t).contains(&Effect::StartCancel(k())));
    assert_eq!(marks(&t)[marks(&t).len() - 2..], [Mark::EdgeTimedOutAuthorization, Mark::EdgeOutcomeNotAcknowledged]);
}

#[test]
fn section_5_the_ledger_call_fails_without_results_answered_as_on_a_timeout_and_the_edge_cancels() {
    let t = run(authorize(&k()), &[(10, Event::CommandAcknowledged), (20, Event::LedgerFailed)]);
    assert_eq!(answers(&t), vec![Reply::TimedOut]);
    assert!(effects(&t).contains(&Effect::StartCancel(k())));
    assert_eq!(marks(&t), vec![Mark::EdgeTimedOutAuthorization, Mark::EdgeLedgerCallFailed]);
}

#[test]
fn section_5_a_paused_edge_finds_the_resolvers_fence_declined_not_executed() {
    let t = run(
        authorize(&k()),
        &[(10, Event::CommandAcknowledged), (20, returned(&[TbStatus::ExistsWithDifferentFlags])), (30, Event::OutcomeAcknowledged)],
    );
    assert_eq!(answers(&t), vec![Reply::Decided(Outcome::Auth(AuthOutcome::Declined(DeclineReason::NotExecuted)))]);
}

#[test]
fn section_8_at_the_deadline_the_authorization_times_out_at_once_and_the_late_decision_is_still_logged() {
    let t = run(authorize(&k()), &[(10, Event::CommandAcknowledged), (1500, Event::TimeAdvanced), (1700, returned(&[TbStatus::Created]))]);
    assert_eq!(answers(&t), vec![Reply::TimedOut]);
    assert!(effects(&t).contains(&Effect::StartCancel(k())));
    assert!(effects(&t).contains(&Effect::AppendOutcome(auth_command(&k()), APPROVED)));
    assert!(marks(&t).contains(&Mark::EdgeLateDecisionLogged));
    assert_eq!(t.last().unwrap().2.state.phase, Phase::Done);
}

#[test]
fn a_late_result_that_never_comes_is_abandoned_to_the_resolver() {
    let t = run(authorize(&k()), &[(10, Event::CommandAcknowledged), (1500, Event::TimeAdvanced), (3500, Event::TimeAdvanced)]);
    assert!(marks(&t).contains(&Mark::EdgeLateResultAbandoned));
    assert!(!effects(&t).iter().any(|e| matches!(e, Effect::AppendOutcome(..))));
}

#[test]
fn section_8_a_capture_that_times_out_is_retried_never_cancelled() {
    let t = run(Request::Capture(k(), CaptureAmount::Full), &[(10, Event::CommandAcknowledged), (1500, Event::TimeAdvanced)]);
    assert_eq!(answers(&t), vec![Reply::Retry]);
    assert!(!effects(&t).iter().any(|e| matches!(e, Effect::StartCancel(_))));
    assert_eq!(marks(&t), vec![Mark::EdgeTimedOutRetry]);
}

#[test]
fn section_8_a_cancel_is_one_call_of_the_fence_and_the_void_fence_first() {
    let t = run(Request::Cancel(k()), &[(10, Event::CommandAcknowledged)]);
    assert_eq!(submits(&t), vec![transfers::cancel(ids::authorization(&k()), &LEDGER_CFG)]);
}

#[test]
fn section_8_the_edges_own_cancel_answers_no_one() {
    let t = run_with(
        Request::Cancel(k()),
        &[
            (10, Event::CommandAcknowledged),
            (20, returned(&[TbStatus::ExistsWithDifferentFlags, TbStatus::Created])),
            (30, Event::OutcomeAcknowledged),
        ],
        Origin::Edge,
        T0,
        &edge_cfg(),
    );
    assert!(answers(&t).is_empty());
    assert!(effects(&t).contains(&Effect::AppendOutcome(cancel_command(&k()), Outcome::Cancel(CancelOutcome::Cancelled))));
}

fn switched() -> edge_request::Config {
    edge_request::Config { periods: PeriodSchedule::new(vec![WallTime::new(T0 + 1000)]), ..edge_cfg() }
}

#[test]
fn section_4_an_authorization_whose_period_has_not_started_is_refused_without_writing() {
    let t = run_with(authorize(&key_at(1, T0 + 2000)), &[], Origin::Caller, T0, &switched());
    assert_eq!(effects(&t), vec![Effect::Answer(Reply::Refused(RefusalReason::PeriodNotStarted))]);
}

#[test]
fn section_4_an_authorization_whose_period_has_ended_is_fenced_and_approved_if_a_copy_created_the_hold() {
    let late = key_at(1, T0);
    let t = run_with(
        authorize(&late),
        &[(10, Event::CommandAcknowledged), (20, returned(&[TbStatus::ExistsWithDifferentFlags])), (30, Event::OutcomeAcknowledged)],
        Origin::Caller,
        T0 + 5000,
        &switched(),
    );
    assert_eq!(submits(&t), vec![vec![transfers::fence(ids::authorization(&late), &LEDGER_CFG)]]);
    assert_eq!(answers(&t), vec![Reply::Decided(APPROVED)]);
    assert!(marks(&t).contains(&Mark::EdgePeriodEndedFencePath));
}

#[test]
fn a_result_the_rules_cannot_place_no_decision_an_alarm_and_the_edge_cancels() {
    let t = run(Request::Cancel(k()), &[(10, Event::CommandAcknowledged), (20, returned(&[TbStatus::Created, TbStatus::Created]))]);
    assert_eq!(answers(&t), vec![Reply::Retry]);
    assert!(effects(&t).iter().any(|e| matches!(e, Effect::Raise(_))));
}

// Properties over generated event sequences, including late, duplicate and out-of-phase events.

fn results(kind: Kind) -> impl Strategy<Value = Vec<TbStatus>> {
    let n = if kind == Kind::Cancel { 2 } else { 1 };
    prop_oneof![9 => proptest::collection::vec(gens::status(), n), 1 => proptest::collection::vec(gens::status(), 0..8)]
}

fn event(kind: Kind) -> impl Strategy<Value = Event> {
    prop_oneof![
        3 => Just(Event::CommandAcknowledged),
        1 => Just(Event::CommandFailed),
        3 => results(kind).prop_map(Event::LedgerReturned),
        1 => Just(Event::LedgerFailed),
        3 => Just(Event::OutcomeAcknowledged),
        1 => Just(Event::OutcomeFailed),
        3 => Just(Event::TimeAdvanced),
    ]
}

fn request() -> impl Strategy<Value = Request> {
    prop_oneof![Just(authorize(&k())), Just(Request::Cancel(k())), gens::capture_amount().prop_map(|a| Request::Capture(k(), a))]
}

fn origin() -> impl Strategy<Value = Origin> {
    prop_oneof![4 => Just(Origin::Caller), 1 => Just(Origin::Edge)]
}

#[derive(Clone, Debug)]
struct Run {
    request: Request,
    origin: Origin,
    trace: Trace,
}

fn traces() -> impl Strategy<Value = Run> {
    (request(), origin())
        .prop_flat_map(|(r, o)| {
            let kind = r.kind();
            (Just(r), Just(o), proptest::collection::vec((0i64..=700, event(kind)), 0..=12))
        })
        .prop_map(|(request, origin, gapped)| {
            let mut at = 0;
            let events: Vec<(i64, Event)> = gapped
                .into_iter()
                .map(|(gap, e)| {
                    at += gap;
                    (at, e)
                })
                .collect();
            let trace = run_with(request.clone(), &events, origin, T0, &edge_cfg());
            Run { request, origin, trace }
        })
}

fn deadline() -> EdgeTime {
    START + edge_cfg().deadline
}

proptest! {
    #[test]
    fn no_ledger_call_comes_before_its_commands_acknowledgement(run in traces()) {
        for (_, e, s) in &run.trace {
            prop_assert!(!s.effects.iter().any(|f| matches!(f, Effect::Submit(..))) || *e == Some(Event::CommandAcknowledged));
        }
    }

    #[test]
    fn no_authorization_reaches_the_ledger_without_time_left_for_the_call_and_the_outcome_write(run in traces()) {
        if run.request.kind() == Kind::Authorization {
            for (at, _, s) in &run.trace {
                prop_assert!(!s.effects.iter().any(|f| matches!(f, Effect::Submit(..))) || *at < START + edge_cfg().hand_off_budget);
            }
        }
    }

    #[test]
    fn no_decision_is_answered_before_its_outcomes_acknowledgement_nor_after_the_deadline(run in traces()) {
        let t = &run.trace;
        let command = t[0].2.state.command.clone();
        for (i, (at, e, s)) in t.iter().enumerate() {
            for f in &s.effects {
                if let Effect::Answer(Reply::Decided(o)) = f {
                    prop_assert!(*e == Some(Event::OutcomeAcknowledged) && *at < deadline());
                    let logged = Effect::AppendOutcome(command.clone(), *o);
                    prop_assert!(t[..i].iter().any(|(_, _, earlier)| earlier.effects.contains(&logged)));
                }
            }
        }
    }

    #[test]
    fn every_request_is_answered_by_its_deadline_once_at_most(run in traces()) {
        let t = &run.trace;
        let first_deadline_tick = t.iter().position(|(at, e, _)| *e == Some(Event::TimeAdvanced) && *at >= deadline());
        let answered_at = t.iter().position(|(_, _, s)| s.effects.iter().any(|f| matches!(f, Effect::Answer(_))));
        if run.origin == Origin::Edge {
            prop_assert!(answers(t).is_empty());
        } else {
            prop_assert!(answers(t).len() <= 1);
            if let Some(tick) = first_deadline_tick {
                prop_assert!(answered_at.is_some_and(|a| a <= tick));
            }
        }
    }

    #[test]
    fn a_captures_or_a_cancels_timeout_never_produces_a_cancel(run in traces()) {
        prop_assert!(run.request.kind() == Kind::Authorization || !effects(&run.trace).iter().any(|f| matches!(f, Effect::StartCancel(_))));
    }

    #[test]
    fn a_refusal_is_final_nothing_reaches_the_ledger_after_it(run in traces()) {
        let t = &run.trace;
        if let Some(refused) = t.iter().position(|(_, _, s)| s.effects.iter().any(|f| matches!(f, Effect::Answer(Reply::Refused(_))))) {
            prop_assert!(!t[refused..].iter().any(|(_, _, s)| s.effects.iter().any(|f| matches!(f, Effect::Submit(..)))));
        }
    }

    #[test]
    fn an_outcome_is_logged_only_from_a_ledger_result(run in traces()) {
        for (_, e, s) in &run.trace {
            prop_assert!(
                !s.effects.iter().any(|f| matches!(f, Effect::AppendOutcome(..))) || matches!(e, Some(Event::LedgerReturned(_)))
            );
        }
    }

    #[test]
    fn a_finished_request_does_nothing_more(run in traces()) {
        let t = &run.trace;
        if let Some(done) = t.iter().position(|(_, _, s)| s.state.phase == Phase::Done) {
            prop_assert!(t[done + 1..].iter().all(|(_, _, s)| s.effects.is_empty()));
        }
    }
}

#[test]
fn the_generated_traces_reach_every_mark_of_the_edges_flow_so_the_properties_are_not_vacuous() {
    let hit: BTreeSet<Mark> = sample(traces(), 3000).iter().flat_map(|run| marks(&run.trace)).collect();
    // The two period marks need a schedule with a switch; the example tests above hit them. The edge process's marks
    // are the edge suite's.
    let left_out = [Mark::EdgeRefusedPeriodNotStarted, Mark::EdgePeriodEndedFencePath, Mark::EdgeOwnCancelDropped, Mark::EdgeReplyAfterEnd];
    let edge_marks: BTreeSet<Mark> = Mark::ALL.iter().copied().filter(|m| m.name().starts_with("Edge") && !left_out.contains(m)).collect();
    assert_eq!(edge_marks.difference(&hit).collect::<Vec<_>>(), Vec::<&Mark>::new());
}
