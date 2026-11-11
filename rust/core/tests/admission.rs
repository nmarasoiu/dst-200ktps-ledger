//! Port of `AdmissionSuite`.

mod common;

use std::collections::BTreeSet;

use common::*;
use ledger_core::admission::{self, Decision, Event, State};
use ledger_core::*;
use proptest::prelude::*;

const CFG: admission::Config = admission::Config::new(2, Span::from_millis(200), 8);

fn k() -> ClientKey {
    key(1)
}

fn partition() -> Partition {
    partitioner::partition(ids::authorization(&k()), CFG.partitions)
}

fn decide(s: &State, r: Request) -> Step<State, Decision> {
    admission::step(s.clone(), Event::Arrived(r), &CFG)
}

fn initial() -> State {
    State::initial(&CFG)
}

fn busy() -> State {
    State { in_flight: 2, limit: 2.0, leaderless: BTreeSet::new() }
}

fn no_leader() -> State {
    State { leaderless: BTreeSet::from([partition()]), ..initial() }
}

fn capture(k: &ClientKey) -> Request {
    Request::Capture(k.clone(), CaptureAmount::Full)
}

#[test]
fn an_authorization_is_admitted_and_counted_in_flight() {
    let s = decide(&initial(), authorize(&k()));
    assert_eq!(s.effects, vec![Decision::Admit]);
    assert_eq!(s.state.in_flight, 1);
    assert_eq!(admission::step(s.state, Event::Finished, &CFG).state.in_flight, 0);
}

#[test]
fn a_partition_without_a_leader_the_authorization_is_refused_at_once_cancels_and_captures_retry() {
    assert_eq!(decide(&no_leader(), authorize(&k())).effects, vec![Decision::Refuse(RefusalReason::NoLeader)]);
    assert_eq!(decide(&no_leader(), authorize(&k())).marks, vec![Mark::AdmissionRefusedNoLeader]);
    assert_eq!(decide(&no_leader(), Request::Cancel(k())).effects, vec![Decision::Retry]);
    assert_eq!(decide(&no_leader(), Request::Cancel(k())).marks, vec![Mark::AdmissionRetryNoLeader]);
    assert_eq!(decide(&no_leader(), capture(&k())).effects, vec![Decision::Retry]);
}

#[test]
fn overloaded_the_authorization_is_refused_the_capture_told_to_retry_the_cancel_accepted() {
    assert_eq!(decide(&busy(), authorize(&k())).effects, vec![Decision::Refuse(RefusalReason::Overload)]);
    assert_eq!(decide(&busy(), authorize(&k())).marks, vec![Mark::AdmissionRefusedOverload]);
    assert_eq!(decide(&busy(), capture(&k())).effects, vec![Decision::Retry]);
    assert_eq!(decide(&busy(), capture(&k())).marks, vec![Mark::AdmissionRetryOverload]);
    let cancel = decide(&busy(), Request::Cancel(k()));
    assert_eq!(cancel.effects, vec![Decision::Admit]);
    assert_eq!(cancel.marks, vec![Mark::AdmissionCancelDuringOverload]);
}

fn heard(s: State, latency: Span, replies: usize) -> State {
    (0..replies).fold(s, |acc, _| admission::step(acc, Event::LedgerLatency(latency), &CFG).state)
}

#[test]
fn slow_replies_lower_the_limit_down_to_one_so_a_slow_ledger_counts_as_overload() {
    let slowed = heard(State { in_flight: 1, ..busy() }, Span::from_millis(400), 40);
    assert_eq!(slowed.limit, 1.0);
    assert_eq!(decide(&slowed, authorize(&k())).effects, vec![Decision::Refuse(RefusalReason::Overload)]);
    assert_eq!(decide(&slowed, capture(&k())).effects, vec![Decision::Retry]);
    assert_eq!(decide(&slowed, Request::Cancel(k())).marks, vec![Mark::AdmissionCancelDuringOverload]);
}

#[test]
fn past_its_headroom_a_cancel_is_told_to_retry_with_nothing_admitted() {
    let full = decide(&State { in_flight: 4, ..busy() }, Request::Cancel(k()));
    assert_eq!(full.effects, vec![Decision::Retry]);
    assert_eq!(full.marks, vec![Mark::AdmissionRetrySaturated]);
    assert_eq!(full.state.in_flight, 4);
}

#[test]
fn fast_replies_raise_a_limit_in_use_never_past_the_ceiling_and_leave_an_idle_one_alone() {
    let low = State { in_flight: 1, limit: 1.0, ..busy() };
    assert!(heard(low.clone(), Span::from_millis(1), 1).limit > 1.0);
    assert_eq!(heard(low.clone(), Span::from_millis(1), 100).limit, f64::from(CFG.max_in_flight));
    let idle = State { in_flight: 0, ..low };
    assert_eq!(heard(idle, Span::from_millis(1), 100).limit, 1.0);
}

#[test]
fn with_nothing_in_flight_one_request_goes_however_slow_the_last_replies_were() {
    let idle = State { in_flight: 0, ..heard(State { in_flight: 1, ..initial() }, Span::from_millis(400), 40) };
    let first = decide(&idle, authorize(&k()));
    assert_eq!(first.effects, vec![Decision::Admit]);
    assert_eq!(decide(&first.state, authorize(&k())).effects, vec![Decision::Refuse(RefusalReason::Overload)]);
}

fn states() -> impl Strategy<Value = State> {
    (
        0i32..=5,
        1.0..=f64::from(CFG.max_in_flight),
        proptest::collection::btree_set((0..CFG.partitions).prop_map(Partition::new), 0..=CFG.partitions as usize),
    )
        .prop_map(|(in_flight, limit, leaderless)| State { in_flight, limit, leaderless })
}

fn requests() -> impl Strategy<Value = Request> {
    gens::key().prop_flat_map(|k| prop_oneof![Just(authorize(&k)), Just(Request::Cancel(k.clone())), Just(capture(&k))])
}

fn leaderless(s: &State, r: &Request) -> bool {
    s.leaderless.contains(&partitioner::partition(r.a(), CFG.partitions))
}

proptest! {
    #[test]
    fn no_threshold_ever_leaves_an_instance_with_nothing_in_flight_unable_to_admit_leaders_permitting(s in states(), r in requests()) {
        prop_assert!(s.in_flight > 0 || leaderless(&s, &r) || decide(&s, r).effects.contains(&Decision::Admit));
    }

    #[test]
    fn a_cancel_is_admitted_wherever_an_authorization_or_a_capture_is(s in states(), k in gens::key()) {
        let others = [authorize(&k), capture(&k)];
        prop_assert!(
            !others.into_iter().any(|r| decide(&s, r).effects.contains(&Decision::Admit))
                || decide(&s, Request::Cancel(k)).effects.contains(&Decision::Admit)
        );
    }

    #[test]
    fn cancels_are_never_refused(s in states(), r in requests()) {
        prop_assert!(r.kind() != Kind::Cancel || !decide(&s, r).effects.iter().any(|d| matches!(d, Decision::Refuse(_))));
    }

    #[test]
    fn a_partition_without_a_leader_gets_no_writes(s in states(), r in requests()) {
        prop_assert!(!leaderless(&s, &r) || !decide(&s, r).effects.contains(&Decision::Admit));
    }

    #[test]
    fn authorizations_are_refused_once_the_limit_is_reached(s in states(), r in requests()) {
        prop_assert!(
            r.kind() != Kind::Authorization
                || f64::from(s.in_flight) < s.limit
                || decide(&s, r).effects.iter().all(|d| matches!(d, Decision::Refuse(_)))
        );
    }

    #[test]
    fn with_nothing_in_flight_a_request_is_admitted_wherever_its_partition_has_a_leader(s in states(), r in requests()) {
        let idle = State { in_flight: 0, ..s };
        prop_assert!(leaderless(&idle, &r) || decide(&idle, r).effects.contains(&Decision::Admit));
    }

    #[test]
    fn the_limit_stays_between_one_and_the_ceiling_whatever_the_ledger_does(
        s in states(),
        latencies in proptest::collection::vec(0i64..=1000, 0..50),
    ) {
        let end = latencies.into_iter().fold(s, |acc, ms| admission::step(acc, Event::LedgerLatency(Span::from_millis(ms)), &CFG).state);
        prop_assert!(end.limit >= 1.0 && end.limit <= f64::from(CFG.max_in_flight));
    }
}
