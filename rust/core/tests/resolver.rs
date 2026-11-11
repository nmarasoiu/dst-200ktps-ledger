//! Port of `ResolverSuite`.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::*;
use ledger_core::ports::command_log::{self, Delivery};
use ledger_core::ports::ledger::{CallResult, Submit};
use ledger_core::resolver::partition::{self, Effect, Event};
use ledger_core::resolver::{self, Ref};
use ledger_core::tb::transfers;
use ledger_core::*;
use proptest::prelude::*;

const P0: Partition = Partition::new(0);

fn step(s: partition::State, now: i64, e: Event) -> Step<partition::State, Effect> {
    partition::step(s, ResolverTime::new(now), e, &resolver_cfg())
}

fn steps(events: Vec<(i64, Event)>) -> Vec<Step<partition::State, Effect>> {
    let mut acc = vec![Step::new(partition::assigned(P0, Generation::new(1), Offset::new(0)))];
    for (now, e) in events {
        let last = acc.last().expect("a step").state.clone();
        acc.push(step(last, now, e));
    }
    acc
}

fn read(offset: i64, at: i64, r: Record) -> (i64, Event) {
    (at, Event::Read(Offset::new(offset), LogTime::new(at), r))
}

fn cmd(c: &Command) -> Record {
    Record::Command(c.clone())
}

fn out(c: &Command, o: Outcome) -> Record {
    Record::Outcome(c.clone(), o, Writer::Edge)
}

fn submits(ss: &[Step<partition::State, Effect>]) -> Vec<Effect> {
    ss.iter().flat_map(|s| s.effects.iter().filter(|e| matches!(e, Effect::Submit(..))).cloned()).collect()
}

fn submitted_ids(ss: &[Step<partition::State, Effect>]) -> Vec<CommandId> {
    submits(ss)
        .into_iter()
        .map(|e| match e {
            Effect::Submit(id, ..) => id,
            _ => unreachable!(),
        })
        .collect()
}

fn auth() -> Command {
    auth_command(&key(1))
}

fn other() -> Command {
    auth_command(&key(2))
}

const APPROVED: Outcome = Outcome::Auth(AuthOutcome::Approved);
const G1: Generation = Generation::new(1);

// Example tests: README section 5, crash rows, and section 8, resolver.

#[test]
fn section_5_a_command_left_without_an_outcome_is_fenced_once_a_record_past_its_due_time_is_read() {
    let due = due();
    let ss = steps(vec![read(0, 0, cmd(&auth())), read(1, due - 1, cmd(&other())), read(2, due, out(&other(), APPROVED))]);
    let fence = vec![transfers::fence(auth().a(), &LEDGER_CFG)];
    assert_eq!(submits(&ss), vec![Effect::Submit(auth().id(), G1, Period::new(0), fence)]);
    assert!(ss.last().unwrap().marks.contains(&Mark::ResolverFencedAuthorization));
    let resolved = steps(vec![
        read(0, 0, cmd(&auth())),
        read(1, due, out(&other(), APPROVED)),
        (due, Event::LedgerReturned(auth().id(), G1, vec![TbStatus::ExistsWithDifferentFlags])),
        (due, Event::OutcomeAcknowledged(auth().id(), G1)),
    ]);
    assert!(resolved[3].effects.contains(&Effect::AppendOutcome(auth().id(), G1, auth(), APPROVED)));
    assert_eq!(resolved.last().unwrap().effects, vec![Effect::Commit(P0, Offset::new(2))]);
}

#[test]
fn section_5_an_edges_outcome_read_within_the_grace_period_no_action_and_the_commit_moves_past_it() {
    let ss = steps(vec![read(0, 0, cmd(&auth())), read(1, 100, out(&auth(), APPROVED)), read(2, due() + 1, cmd(&other()))]);
    assert!(submits(&ss).is_empty());
    assert_eq!(ss[2].effects, vec![Effect::Commit(P0, Offset::new(2))]);
    assert!(ss[2].marks.contains(&Mark::ResolverFoundOutcome));
}

#[test]
fn the_commit_never_passes_the_oldest_command_without_an_outcome() {
    let ss = steps(vec![read(0, 0, cmd(&auth())), read(1, 10, cmd(&other())), read(2, 20, out(&other(), APPROVED))]);
    assert!(!ss.iter().flat_map(|s| &s.effects).any(|e| matches!(e, Effect::Commit(..))));
}

#[test]
fn an_idle_partition_is_judged_by_the_resolvers_clock_once_its_end_is_reached_past_the_due_time() {
    let early = steps(vec![read(0, 0, cmd(&auth())), (due() - 1, Event::ReachedEnd)]);
    assert!(submits(&early).is_empty());
    let late = steps(vec![read(0, 0, cmd(&auth())), (due(), Event::ReachedEnd)]);
    assert_eq!(submitted_ids(&late), vec![auth().id()]);
    assert!(late.last().unwrap().marks.contains(&Mark::ResolverJudgedIdlePartition));
}

#[test]
fn an_end_reached_before_the_due_time_does_not_count_later_the_clock_alone_never_judges() {
    let ss = steps(vec![read(0, 0, cmd(&auth())), (100, Event::ReachedEnd), (due() + 60_000, Event::TimeAdvanced)]);
    assert!(submits(&ss).is_empty());
}

#[test]
fn section_5_an_outcome_read_while_the_fence_is_in_flight_the_late_result_is_ignored_no_duplicate() {
    let due = due();
    let ss = steps(vec![
        read(0, 0, cmd(&auth())),
        (due, Event::ReachedEnd),
        read(1, due + 5, out(&auth(), APPROVED)),
        (due + 6, Event::LedgerReturned(auth().id(), G1, vec![TbStatus::ExistsWithDifferentFlags])),
    ]);
    assert!(ss[3].marks.contains(&Mark::ResolverOutcomeWhileInFlight));
    assert!(!ss.iter().flat_map(|s| &s.effects).any(|e| matches!(e, Effect::AppendOutcome(..))));
    assert_eq!(ss.last().unwrap().marks, vec![Mark::ResolverStaleResult]);
}

#[test]
fn section_8_a_straggling_cancel_is_resubmitted_as_the_fence_and_the_void_a_capture_as_its_post() {
    let cancel = cancel_command(&key(3));
    let capture = Command::Capture(key(4), CaptureAmount::partial(5));
    let ss = steps(vec![read(0, 0, cmd(&cancel)), read(1, 0, cmd(&capture)), (due(), Event::ReachedEnd)]);
    let sent: Vec<Vec<Transfer>> = submits(&ss)
        .into_iter()
        .map(|e| match e {
            Effect::Submit(_, _, _, ts) => ts,
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(sent, vec![transfers::cancel(cancel.a(), &LEDGER_CFG), vec![transfers::post(capture.a(), CaptureAmount::partial(5))]]);
}

#[test]
fn a_result_the_rules_cannot_place_is_retried_after_the_backoff_with_an_alarm() {
    let due = due();
    let ss = steps(vec![
        read(0, 0, cmd(&auth())),
        (due, Event::ReachedEnd),
        (due, Event::LedgerReturned(auth().id(), G1, vec![TbStatus::LedgerMustNotBeZero])),
        (due + 100, Event::TimeAdvanced),
        (due + 500, Event::TimeAdvanced),
    ]);
    assert!(ss[3].effects.iter().any(|e| matches!(e, Effect::Raise(_))));
    assert!(ss[3].effects.contains(&Effect::WakeAt(ResolverTime::new(due + 500))));
    assert!(submits(&ss[4..5]).is_empty());
    assert_eq!(submitted_ids(&ss[5..6]), vec![auth().id()]);
}

#[test]
fn records_read_again_below_the_next_offset_are_ignored() {
    let ss = steps(vec![read(0, 0, cmd(&auth())), read(0, 0, cmd(&auth()))]);
    assert_eq!(ss.last().unwrap().marks, vec![Mark::ResolverRedelivered]);
}

fn scan(events: Vec<resolver::Event>, now: ResolverTime) -> Vec<Step<resolver::State, resolver::Effect>> {
    let cfg = resolver_cfg();
    let mut acc = vec![Step::new(resolver::State::INITIAL)];
    for e in events {
        let last = acc.last().expect("a step").state.clone();
        acc.push(resolver::step(last, now, e, &cfg));
    }
    acc
}

#[test]
fn section_5_a_zombies_results_from_before_a_rebalance_are_dropped() {
    use resolver::Event::Delivered;
    let now = ResolverTime::new(due());
    let ss = scan(
        vec![
            Delivered(Delivery::Assigned(P0, Offset::new(0))),
            Delivered(Delivery::Read(P0, Offset::new(0), LogTime::new(0), cmd(&auth()))),
            Delivered(Delivery::ReachedEnd(P0)),
            Delivered(Delivery::Revoked(P0)),
            Delivered(Delivery::Assigned(P0, Offset::new(0))),
            resolver::Event::Returned(
                Ref { partition: P0, id: auth().id(), generation: G1 },
                CallResult::Returned(vec![TbStatus::Created]),
            ),
        ],
        now,
    );
    // The Scala suite starts from the first assignment, so its steps are one behind these.
    let generations: Vec<Generation> = ss[3]
        .effects
        .iter()
        .filter_map(|e| match e {
            resolver::Effect::Submit(r, _) => Some(r.generation),
            _ => None,
        })
        .collect();
    assert_eq!(generations, vec![G1]);
    assert_eq!(ss[4].marks, vec![Mark::ResolverRevoked]);
    assert!(ss.last().unwrap().effects.is_empty());
    assert_eq!(ss.last().unwrap().marks, vec![Mark::ResolverStaleResult]);
}

#[test]
fn the_process_speaks_the_ports_a_fence_goes_out_with_its_ref_and_its_reply_comes_back_through_it() {
    use resolver::Event::Delivered;
    let r = Ref { partition: P0, id: auth().id(), generation: G1 };
    let m = resolver::machine(resolver_cfg());
    let events = vec![
        Delivered(Delivery::Assigned(P0, Offset::new(0))),
        Delivered(Delivery::Read(P0, Offset::new(0), LogTime::new(0), cmd(&auth()))),
        Delivered(Delivery::ReachedEnd(P0)),
        resolver::Event::Returned(r, CallResult::Returned(vec![TbStatus::Created])),
        resolver::Event::Written(r, command_log::Written::Acknowledged),
    ];
    let mut ss = vec![Step::new(m.initial.clone())];
    for e in events {
        let last = ss.last().expect("a step").state.clone();
        ss.push(m.step(last, ResolverTime::new(due()), e));
    }
    let fence = Submit { cluster: Period::new(0), transfers: vec![transfers::fence(auth().a(), &LEDGER_CFG)] };
    assert_eq!(ss[3].effects, vec![resolver::Effect::Submit(r, fence)]);
    let decided = Record::Outcome(auth(), Outcome::Auth(AuthOutcome::Declined(DeclineReason::NotExecuted)), Writer::Resolver);
    assert_eq!(ss[4].effects, vec![resolver::Effect::Append(r, command_log::Append { record: decided })]);
    assert_eq!(ss[5].effects, vec![resolver::Effect::Commit(command_log::Commit { partition: P0, next: Offset::new(1) })]);
}

// Properties over generated logs, with a small driver that answers the resolver's calls and writes.

#[derive(Clone, Debug)]
struct Entry {
    appended: i64,
    record: Record,
}

fn command_of(n: i32, kind: Kind) -> Command {
    match kind {
        Kind::Authorization => auth_command(&key(n)),
        Kind::Cancel => cancel_command(&key(n)),
        Kind::Capture => capture_command(&key(n)),
    }
}

fn outcome_of(kind: Kind) -> Outcome {
    match kind {
        Kind::Authorization => APPROVED,
        Kind::Cancel => Outcome::Cancel(CancelOutcome::Cancelled),
        Kind::Capture => Outcome::Capture(CaptureOutcome::Captured(CaptureAmount::Full)),
    }
}

/// Commands on one partition, each with an outcome that may come after `max_delay` or never.
fn logs(max_delay: i64, always_outcome: bool) -> impl Strategy<Value = Vec<Entry>> {
    let delay = if always_outcome { (0..=max_delay).prop_map(Some).boxed() } else { proptest::option::of(0..=max_delay).boxed() };
    let kind = prop_oneof![Just(Kind::Authorization), Just(Kind::Cancel), Just(Kind::Capture)];
    let command = (1i32..=4, kind, 0i64..=2000, delay).prop_map(|(n, kind, gap, delay)| (command_of(n, kind), gap, delay));
    proptest::collection::vec(command, 1..=15).prop_map(|cs| {
        let mut t = 0;
        let mut entries = Vec::new();
        for (c, gap, delay) in cs {
            t += gap;
            entries.push(Entry { appended: t, record: Record::Command(c.clone()) });
            if let Some(d) = delay {
                let o = outcome_of(c.kind());
                entries.push(Entry { appended: t + d, record: Record::Outcome(c, o, Writer::Edge) });
            }
        }
        // Stable, as Scala's `sortBy` is.
        entries.sort_by_key(|e| e.appended);
        entries
    })
}

struct Driven {
    effects: Vec<Effect>,
    marks: Vec<Mark>,
    commits_past_waiting: Vec<String>,
    end: partition::State,
}

/// Reads the log from `from`, then reaches its end long after the last record. A reference model tracks the commands
/// still without an outcome, to check every commit against.
struct Driver<'a> {
    commands: BTreeMap<CommandId, Command>,
    answer: &'a dyn Fn(&Command) -> Vec<TbStatus>,
    state: Option<partition::State>,
    waiting: BTreeMap<CommandId, i64>,
    effects: Vec<Effect>,
    marks: Vec<Mark>,
    broken: Vec<String>,
}

impl Driver<'_> {
    fn process(&mut self, now: i64, e: Event) {
        match &e {
            Event::Read(o, _, Record::Command(c)) => {
                self.waiting.entry(c.id()).or_insert(o.value());
            }
            Event::Read(_, _, Record::Outcome(c, _, _)) => {
                self.waiting.remove(&c.id());
            }
            Event::OutcomeAcknowledged(id, _) => {
                self.waiting.remove(id);
            }
            _ => {}
        }
        let st = step(self.state.take().expect("a state"), now, e);
        self.state = Some(st.state);
        self.effects.extend(st.effects.iter().cloned());
        self.marks.extend(st.marks.iter().copied());
        for f in &st.effects {
            if let Effect::Commit(_, next) = f
                && self.waiting.values().any(|o| *o < next.value())
            {
                self.broken.push(format!("commit {next:?} passes {:?}", self.waiting));
            }
        }
        for f in st.effects {
            match f {
                Effect::Submit(id, g, _, _) => {
                    let statuses = (self.answer)(&self.commands[&id]);
                    self.process(now, Event::LedgerReturned(id, g, statuses));
                }
                Effect::AppendOutcome(id, g, _, _) => self.process(now, Event::OutcomeAcknowledged(id, g)),
                _ => {}
            }
        }
    }
}

fn drive(log: &[Entry], from: usize, answer: &dyn Fn(&Command) -> Vec<TbStatus>) -> Driven {
    let mut d = Driver {
        commands: log.iter().map(|e| (e.record.command().id(), e.record.command().clone())).collect(),
        answer,
        state: Some(partition::assigned(P0, G1, Offset::new(from as i64))),
        waiting: BTreeMap::new(),
        effects: Vec::new(),
        marks: Vec::new(),
        broken: Vec::new(),
    };
    for (i, entry) in log.iter().enumerate().skip(from) {
        d.process(entry.appended, Event::Read(Offset::new(i as i64), LogTime::new(entry.appended), entry.record.clone()));
    }
    let end = log.last().map_or(0, |e| e.appended) + due() + 10_000;
    d.process(end, Event::ReachedEnd);
    Driven { effects: d.effects, marks: d.marks, commits_past_waiting: d.broken, end: d.state.expect("a state") }
}

fn well_formed(c: &Command) -> Vec<TbStatus> {
    match c {
        Command::Authorize(_) => vec![TbStatus::Created],
        Command::Cancel(_) => vec![TbStatus::Created, TbStatus::PendingTransferNotPending],
        Command::Capture(..) => vec![TbStatus::Created],
    }
}

/// Any statuses, as the Scala `anyAnswers`: three of them, and a command takes the first two if it is a cancel, else
/// one picked by the low bit of its `A`.
fn any_answers() -> impl Strategy<Value = Vec<TbStatus>> {
    proptest::collection::vec(gens::status(), 3)
}

fn answering(ss: &[TbStatus]) -> impl Fn(&Command) -> Vec<TbStatus> + '_ {
    move |c| if c.kind() == Kind::Cancel { ss[..2].to_vec() } else { vec![ss[(c.a().value() & 1) as usize]] }
}

proptest! {
    #[test]
    fn the_resolver_never_submits_an_authorization_it_fences_it(log in logs(8000, false), ss in any_answers()) {
        for e in drive(&log, 0, &answering(&ss)).effects {
            if let Effect::Submit(_, _, _, ts) = e {
                prop_assert!(!ts.iter().any(Transfer::is_pending));
            }
        }
    }

    #[test]
    fn no_offset_commit_passes_a_command_without_an_outcome(log in logs(8000, false), ss in any_answers()) {
        let driven = drive(&log, 0, &answering(&ss));
        prop_assert!(driven.commits_past_waiting.is_empty(), "{:?}", driven.commits_past_waiting);
    }

    #[test]
    fn every_straggler_gets_an_outcome_once_its_grace_period_has_passed_in_log_time(log in logs(8000, false)) {
        let driven = drive(&log, 0, &well_formed);
        let written: BTreeSet<CommandId> = driven
            .effects
            .iter()
            .filter_map(|e| match e {
                Effect::AppendOutcome(id, ..) => Some(*id),
                _ => None,
            })
            .collect();
        for (i, entry) in log.iter().enumerate() {
            if let Record::Command(c) = &entry.record {
                let resolved_later = log[i + 1..].iter().any(|e| matches!(e.record, Record::Outcome(..)) && e.record.command().id() == c.id());
                prop_assert!(resolved_later || written.contains(&c.id()));
            }
        }
        prop_assert!(driven.end.waiting.is_empty());
        prop_assert_eq!(driven.end.committed, Offset::new(log.len() as i64));
    }

    #[test]
    fn a_resolver_reading_again_from_any_offset_acts_only_on_commands_with_no_outcome_in_the_log(
        (log, from) in logs(due() - 1, true).prop_flat_map(|log| { let n = log.len(); (Just(log), 0..=n) }),
        ss in any_answers(),
    ) {
        prop_assert!(!drive(&log, from, &answering(&ss)).effects.iter().any(|e| matches!(e, Effect::Submit(..))));
    }
}

#[test]
fn the_generated_logs_reach_the_resolvers_marks_so_the_properties_are_not_vacuous() {
    let hit: BTreeSet<Mark> =
        sample((logs(8000, false), any_answers()), 300).iter().flat_map(|(log, ss)| drive(log, 0, &answering(ss)).marks).collect();
    let expected = BTreeSet::from([
        Mark::ResolverFencedAuthorization,
        Mark::ResolverResubmittedCancel,
        Mark::ResolverResubmittedCapture,
        Mark::ResolverJudgedIdlePartition,
        Mark::ResolverFoundOutcome,
        Mark::ResolverRetryScheduled,
    ]);
    assert_eq!(expected.difference(&hit).collect::<Vec<_>>(), Vec::<&Mark>::new());
}
