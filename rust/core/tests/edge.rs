//! Port of `EdgeSuite`.

mod common;

use std::collections::BTreeSet;

use common::*;
use ledger_core::edge::{self, Effect, Event, Ref};
use ledger_core::ports::command_log::{self, Append, Leaders};
use ledger_core::ports::ledger::{CallResult, Submit};
use ledger_core::tb::transfers;
use ledger_core::*;
use proptest::prelude::*;

fn cfg() -> edge::Config {
    edge::Config { request: edge_cfg(), admission: admission::Config::new(2, Span::from_millis(200), 8) }
}

fn k() -> ClientKey {
    key(1)
}

const C1: Connection = Connection::new(1);
const CALLER: Ref = Ref::Caller(C1);

fn partition() -> Partition {
    partitioner::partition(ids::authorization(&k()), cfg().admission.partitions)
}

fn at(t: i64) -> edge::Now {
    edge::Now { monotonic: EdgeTime::new(t), wall: WallTime::new(T0 + t) }
}

/// Each event at its time, from a fresh instance. The first step is the initial state.
fn steps(events: Vec<(i64, Event)>) -> Vec<Step<edge::State, Effect>> {
    let cfg = cfg();
    let mut acc = vec![Step::new(edge::State::initial(&cfg))];
    for (t, e) in events {
        let last = acc.last().expect("a step").state.clone();
        acc.push(edge::step(last, at(t), e, &cfg));
    }
    acc
}

fn acked(r: Ref, record: Record) -> Event {
    Event::Written(r, record, command_log::Written::Acknowledged)
}

fn returned(r: Ref, statuses: &[TbStatus]) -> Event {
    Event::Returned(r, CallResult::Returned(statuses.to_vec()))
}

fn append(r: Ref, record: Record) -> Effect {
    Effect::Append(r, Append { record })
}

fn outcome(c: &Command, o: Outcome) -> Record {
    Record::Outcome(c.clone(), o, Writer::Edge)
}

fn without_wakes(effects: Vec<Effect>) -> Vec<Effect> {
    effects.into_iter().filter(|e| !matches!(e, Effect::WakeAt(..))).collect()
}

fn all_effects(ss: &[Step<edge::State, Effect>]) -> Vec<Effect> {
    ss.iter().flat_map(|s| s.effects.clone()).collect()
}

const APPROVED: Outcome = Outcome::Auth(AuthOutcome::Approved);
const CANCELLED: Outcome = Outcome::Cancel(CancelOutcome::Cancelled);

// Example tests: how the request flow and admission control meet (README section 11, processes).

#[test]
fn an_admitted_authorization_log_call_the_ledger_log_the_outcome_answer_then_leave_the_count() {
    let auth = auth_command(&k());
    let ss = steps(vec![
        (0, Event::Arrived(C1, authorize(&k()))),
        (10, acked(CALLER, Record::Command(auth.clone()))),
        (20, returned(CALLER, &[TbStatus::Created])),
        (30, acked(CALLER, outcome(&auth, APPROVED))),
    ]);
    assert_eq!(
        without_wakes(all_effects(&ss)),
        vec![
            append(CALLER, Record::Command(auth.clone())),
            Effect::Submit(CALLER, Submit { cluster: Period::new(0), transfers: transfers::for_edge(&auth, false, &LEDGER_CFG) }),
            append(CALLER, outcome(&auth, APPROVED)),
            Effect::Answer(C1, Reply::Decided(APPROVED)),
        ]
    );
    assert_eq!(ss[1].state.admission.in_flight, 1);
    assert!(ss.last().unwrap().state.requests.is_empty());
    assert_eq!(ss.last().unwrap().state.admission.in_flight, 0);
}

#[test]
fn overload_an_authorization_is_refused_at_once_with_nothing_written_a_cancel_is_still_admitted() {
    let ss = steps(vec![
        (0, Event::Arrived(Connection::new(1), authorize(&key(1)))),
        (0, Event::Arrived(Connection::new(2), authorize(&key(2)))),
        (0, Event::Arrived(Connection::new(3), authorize(&key(3)))),
        (0, Event::Arrived(Connection::new(4), Request::Cancel(key(4)))),
    ]);
    assert_eq!(ss[3].effects, vec![Effect::Answer(Connection::new(3), Reply::Refused(RefusalReason::Overload))]);
    let appended: Vec<Ref> = ss[4]
        .effects
        .iter()
        .filter_map(|e| match e {
            Effect::Append(r, _) => Some(*r),
            _ => None,
        })
        .collect();
    assert_eq!(appended, vec![Ref::Caller(Connection::new(4))]);
    assert_eq!(ss.last().unwrap().state.admission.in_flight, 3);
}

#[test]
fn a_partition_without_a_leader_the_authorization_is_refused_the_cancel_and_the_capture_retry_nothing_written() {
    let ss = steps(vec![
        (0, Event::LeadersChanged(Leaders { leaderless: BTreeSet::from([partition()]) })),
        (0, Event::Arrived(Connection::new(1), authorize(&k()))),
        (0, Event::Arrived(Connection::new(2), Request::Cancel(k()))),
        (0, Event::Arrived(Connection::new(3), Request::Capture(k(), CaptureAmount::Full))),
    ]);
    assert_eq!(
        all_effects(&ss),
        vec![
            Effect::Answer(Connection::new(1), Reply::Refused(RefusalReason::NoLeader)),
            Effect::Answer(Connection::new(2), Reply::Retry),
            Effect::Answer(Connection::new(3), Reply::Retry),
        ]
    );
}

#[test]
fn section_8_on_its_timeout_the_edge_answers_timed_out_and_sends_its_own_cancel_which_answers_no_one() {
    let auth = auth_command(&k());
    let cancel = cancel_command(&k());
    let own = Ref::OwnCancel(C1);
    let ss = steps(vec![
        (0, Event::Arrived(C1, authorize(&k()))),
        (10, acked(CALLER, Record::Command(auth))),
        (1500, Event::Woke(CALLER)),
        (1510, acked(own, Record::Command(cancel.clone()))),
        (1520, returned(own, &[TbStatus::ExistsWithDifferentFlags, TbStatus::Created])),
        (1530, acked(own, outcome(&cancel, CANCELLED))),
    ]);
    assert_eq!(
        without_wakes(ss[3].effects.clone()),
        vec![Effect::Answer(C1, Reply::TimedOut), append(own, Record::Command(cancel.clone()))]
    );
    // The authorization still waits for its late result, so both count in flight.
    assert_eq!(ss[3].state.admission.in_flight, 2);
    assert_eq!(ss[5].effects, vec![append(own, outcome(&cancel, CANCELLED))]);
    assert!(ss[6].effects.is_empty());
    assert_eq!(ss[6].state.requests.keys().copied().collect::<Vec<_>>(), vec![CALLER]);
}

#[test]
fn the_edges_own_cancel_is_dropped_when_its_partition_has_no_leader() {
    let ss = steps(vec![
        (0, Event::Arrived(C1, authorize(&k()))),
        (10, acked(CALLER, Record::Command(auth_command(&k())))),
        (20, Event::LeadersChanged(Leaders { leaderless: BTreeSet::from([partition()]) })),
        (1500, Event::Woke(CALLER)),
    ]);
    assert_eq!(without_wakes(ss[4].effects.clone()), vec![Effect::Answer(C1, Reply::TimedOut)]);
    assert!(ss[4].marks.contains(&Mark::EdgeOwnCancelDropped));
    assert_eq!(ss[4].state.admission.in_flight, 1);
}

#[test]
fn the_ledgers_latency_from_each_call_to_its_reply_feeds_admission_control() {
    // Eight authorizations in turn, each with a 400 ms ledger call, past the target of 200 ms, bring the limit to one.
    let mut events = Vec::new();
    for n in 1..=8 {
        let (c, kn, t) = (Connection::new(n), key(n as i32), n * 1000);
        events.push((t, Event::Arrived(c, authorize(&kn))));
        events.push((t + 1, acked(Ref::Caller(c), Record::Command(auth_command(&kn)))));
        events.push((t + 401, returned(Ref::Caller(c), &[TbStatus::Created])));
        events.push((t + 402, acked(Ref::Caller(c), outcome(&auth_command(&kn), APPROVED))));
    }
    events.push((9000, Event::Arrived(Connection::new(9), authorize(&key(9)))));
    events.push((9001, Event::Arrived(Connection::new(10), authorize(&key(10)))));
    let ss = steps(events);
    let answers: Vec<Reply> = all_effects(&ss)
        .into_iter()
        .filter_map(|e| match e {
            Effect::Answer(_, r) => Some(r),
            _ => None,
        })
        .take(8)
        .collect();
    assert_eq!(answers, vec![Reply::Decided(APPROVED); 8]);
    assert_eq!(ss.last().unwrap().state.admission.limit, 1.0);
    // With nothing in flight one goes, and its reply is the news. The next one waits for that news.
    assert!(ss[ss.len() - 2].effects.iter().any(|e| matches!(e, Effect::Append(..))));
    assert_eq!(ss.last().unwrap().effects, vec![Effect::Answer(Connection::new(10), Reply::Refused(RefusalReason::Overload))]);
}

#[test]
fn a_ledger_call_abandoned_without_a_reply_counts_at_the_time_it_waited() {
    let ss = steps(vec![
        (0, Event::Arrived(C1, authorize(&k()))),
        (10, acked(CALLER, Record::Command(auth_command(&k())))),
        (1500, Event::Woke(CALLER)),
        (3500, Event::Woke(CALLER)),
        (3600, Event::Arrived(Connection::new(2), authorize(&key(2)))),
    ]);
    assert!(ss[4].marks.contains(&Mark::EdgeLateResultAbandoned));
    assert_eq!(ss[5].effects, vec![Effect::Answer(Connection::new(2), Reply::Refused(RefusalReason::Overload))]);
}

#[test]
fn an_abandoned_call_stays_counted_until_it_replies_since_it_still_takes_the_ledgers_time() {
    let ss = steps(vec![
        (0, Event::Arrived(C1, authorize(&k()))),
        (10, acked(CALLER, Record::Command(auth_command(&k())))),
        (1500, Event::Woke(CALLER)),
        (3500, Event::Woke(CALLER)),
        (9000, returned(CALLER, &[TbStatus::Created])),
    ]);
    assert_eq!(ss[4].state.abandoned, BTreeSet::from([CALLER]));
    assert!(!ss[4].state.requests.contains_key(&CALLER));
    assert!(ss[5].state.abandoned.is_empty());
    assert_eq!(ss[5].state.admission.in_flight, ss[4].state.admission.in_flight - 1);
    assert!(ss[5].effects.is_empty());
    assert_eq!(ss[5].marks, vec![Mark::EdgeReplyAfterEnd]);
    // It counted once as a sample, when it was abandoned.
    assert!(ss[4].state.admission.limit < ss[3].state.admission.limit);
    assert_eq!(ss[5].state.admission.limit, ss[4].state.admission.limit);
}

#[test]
fn a_reply_for_a_request_that_has_ended_changes_nothing() {
    let auth = auth_command(&k());
    let ss = steps(vec![
        (0, Event::Arrived(C1, authorize(&k()))),
        (10, acked(CALLER, Record::Command(auth.clone()))),
        (1500, Event::Woke(CALLER)),
        (1600, returned(CALLER, &[TbStatus::Created])),
        (1700, acked(CALLER, outcome(&auth, APPROVED))),
    ]);
    // The late decision is logged after the timeout, and its acknowledgement finds the request ended.
    assert_eq!(ss[4].effects, vec![append(CALLER, outcome(&auth, APPROVED))]);
    assert!(ss[5].effects.is_empty());
    assert_eq!(ss[5].marks, vec![Mark::EdgeReplyAfterEnd]);
}

// Properties over generated runs, with replies coming back in any order.

#[derive(Clone, Debug)]
enum Action {
    Arrive(Request),
    WriteReply(usize, command_log::Written),
    LedgerReply(usize, CallResult),
    Wake(usize),
    Leaders(BTreeSet<Partition>),
}

fn requests() -> impl Strategy<Value = Request> {
    (1i32..=4).prop_flat_map(|n| {
        let kn = key(n);
        prop_oneof![Just(authorize(&kn)), Just(Request::Cancel(kn.clone())), Just(Request::Capture(kn, CaptureAmount::Full))]
    })
}

fn actions() -> impl Strategy<Value = Vec<(i64, Action)>> {
    let written = prop_oneof![5 => Just(command_log::Written::Acknowledged), 1 => Just(command_log::Written::Failed)];
    let statuses = proptest::collection::vec(gens::status(), 1..=2).prop_map(CallResult::Returned);
    let result = prop_oneof![5 => statuses, 1 => Just(CallResult::Failed)];
    let partitions = cfg().admission.partitions;
    let leaderless = proptest::collection::btree_set((0..partitions).prop_map(Partition::new), 0..=partitions as usize);
    let action = prop_oneof![
        3 => requests().prop_map(Action::Arrive),
        4 => (0usize..100, written).prop_map(|(i, w)| Action::WriteReply(i, w)),
        3 => (0usize..100, result).prop_map(|(i, r)| Action::LedgerReply(i, r)),
        2 => (0usize..100).prop_map(Action::Wake),
        1 => leaderless.prop_map(Action::Leaders),
    ];
    proptest::collection::vec((0i64..=600, action), 0..=40)
}

/// Removes the element `i` modulo the length, if there is one.
fn take<A>(xs: &mut Vec<A>, i: usize) -> Option<A> {
    if xs.is_empty() { None } else { Some(xs.remove(i % xs.len())) }
}

/// Runs the actions through the edge's machine. Each reply goes to a call or write still without one, picked by
/// index, and a wake-up moves the clock to its time. Returns each step with the state before it.
fn drive(actions: &[(i64, Action)]) -> Vec<(edge::State, Event, Step<edge::State, Effect>)> {
    let m = edge::machine(cfg());
    let mut s = m.initial.clone();
    let mut t = 0;
    let mut connections = 0;
    let mut writes: Vec<(Ref, Append)> = Vec::new();
    let mut calls: Vec<Ref> = Vec::new();
    let mut wakes: Vec<(Ref, EdgeTime)> = Vec::new();
    let mut out = Vec::new();
    for (gap, action) in actions {
        t += gap;
        let event = match action {
            Action::Arrive(r) => {
                connections += 1;
                Some(Event::Arrived(Connection::new(connections), r.clone()))
            }
            Action::WriteReply(i, result) => take(&mut writes, *i).map(|(r, a)| Event::Written(r, a.record, *result)),
            Action::LedgerReply(i, result) => take(&mut calls, *i).map(|r| Event::Returned(r, result.clone())),
            Action::Wake(i) => take(&mut wakes, *i).map(|(r, when)| {
                t = t.max(when.millis());
                Event::Woke(r)
            }),
            Action::Leaders(leaderless) => Some(Event::LeadersChanged(Leaders { leaderless: leaderless.clone() })),
        };
        if let Some(e) = event {
            let st = m.step(s.clone(), at(t), e.clone());
            for f in &st.effects {
                match f {
                    Effect::Append(r, a) => writes.push((*r, a.clone())),
                    Effect::Submit(r, _) => calls.push(*r),
                    Effect::WakeAt(r, when) => wakes.push((*r, *when)),
                    _ => {}
                }
            }
            out.push((s, e, st.clone()));
            s = st.state;
        }
    }
    out
}

proptest! {
    #[test]
    fn admission_control_counts_exactly_the_requests_in_flight_and_the_calls_they_abandoned(actions in actions()) {
        for (_, _, st) in drive(&actions) {
            prop_assert_eq!(st.state.admission.in_flight as usize, st.state.requests.len() + st.state.abandoned.len());
        }
    }

    #[test]
    fn a_request_whose_partition_has_no_leader_is_answered_at_once_with_nothing_written(actions in actions()) {
        let partitions = cfg().admission.partitions;
        for (before, e, st) in drive(&actions) {
            if let Event::Arrived(_, r) = e
                && before.admission.leaderless.contains(&partitioner::partition(r.a(), partitions))
            {
                prop_assert!(!st.effects.iter().any(|f| matches!(f, Effect::Append(..))));
                prop_assert!(st.effects.iter().any(|f| matches!(f, Effect::Answer(..))));
            }
        }
    }

    #[test]
    fn each_connection_that_arrived_is_answered_at_most_once_and_no_other_is(actions in actions()) {
        let run = drive(&actions);
        let arrived: BTreeSet<Connection> = run
            .iter()
            .filter_map(|(_, e, _)| match e {
                Event::Arrived(c, _) => Some(*c),
                _ => None,
            })
            .collect();
        let answered: Vec<Connection> = run
            .iter()
            .flat_map(|(_, _, st)| &st.effects)
            .filter_map(|f| match f {
                Effect::Answer(c, _) => Some(*c),
                _ => None,
            })
            .collect();
        let distinct: BTreeSet<Connection> = answered.iter().copied().collect();
        prop_assert_eq!(distinct.len(), answered.len());
        prop_assert!(answered.iter().all(|c| arrived.contains(c)));
    }
}

#[test]
fn the_generated_runs_reach_the_edge_processs_marks_so_the_properties_are_not_vacuous() {
    let hit: BTreeSet<Mark> =
        sample(actions(), 500).iter().flat_map(|actions| drive(actions).into_iter().flat_map(|(_, _, st)| st.marks)).collect();
    let expected = BTreeSet::from([
        Mark::EdgeOwnCancelDropped,
        Mark::EdgeReplyAfterEnd,
        Mark::EdgeTimedOutAuthorization,
        Mark::AdmissionRefusedOverload,
        Mark::AdmissionRefusedNoLeader,
    ]);
    assert_eq!(expected.difference(&hit).collect::<Vec<_>>(), Vec::<&Mark>::new());
}
