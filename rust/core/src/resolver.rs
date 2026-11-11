//! The resolver process: one `partition` machine per assigned partition of the command log (README section 8,
//! resolver). On a rebalance it drops the work in flight; reading again from the committed offset rebuilds the state
//! (README section 8, failover). It speaks the ports: the consumer's deliveries come in, and each call and write
//! carries its `Ref`, which the reply carries back to its partition and command. Port of `Resolver.scala`.

use alloc::collections::BTreeMap;
use alloc::vec;
use alloc::vec::Vec;

use crate::domain::{Alarm, CommandId, Record, Writer};
use crate::machine::{Machine, ResolverTime, Step};
use crate::mark::Mark;
use crate::partitioner::Partition;
use crate::ports::command_log::{Append, Commit, Delivery, Written};
use crate::ports::ledger::{CallResult, Submit};

/// One assignment of a partition to the resolver. Results tagged with an older one, from before a rebalance, are
/// dropped. Never mistaken for an offset or a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(i64);

impl Generation {
    pub const INITIAL: Generation = Generation(0);

    pub const fn new(n: i64) -> Generation {
        Generation(n)
    }

    pub const fn next(self) -> Generation {
        Generation(self.0 + 1)
    }
}

/// The resolver on one assigned partition of the command log. It gives an outcome to every command still without
/// one a grace period after its deadline, judged in log time; it fences an authorization and resubmits a cancel or a
/// capture. It never submits an authorization, and it commits offsets only up to its oldest command still without
/// an outcome. Port of `ResolverPartition`.
pub mod partition {
    use super::*;
    use alloc::collections::btree_map::Entry;

    use crate::domain::{Command, Outcome};
    use crate::machine::{LogTime, Span};
    use crate::partitioner::Offset;
    use crate::periods::{Period, PeriodSchedule};
    use crate::rules::{self, Verdict};
    use crate::tb::{LedgerConfig, TbStatus, Transfer, transfers};

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Config {
        pub deadline: Span,
        pub grace: Span,
        /// How long to wait before trying again after a failed call, a failed write or an unresolved result.
        pub retry_backoff: Span,
        pub periods: PeriodSchedule,
        pub ledger: LedgerConfig,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum Phase {
        Idle(Option<ResolverTime>),
        Submitting,
        Appending(Outcome),
    }

    /// A command read with no outcome after it yet. `first` and `appended` are those of its earliest copy.
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct Waiting {
        pub first: Offset,
        pub appended: LogTime,
        pub command: Command,
        pub phase: Phase,
    }

    /// `log_now` is the latest append time read: the partition's own clock. `end_seen_at` is when a poll last
    /// reached the end of the partition, by the resolver's clock; a new record clears it.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct State {
        pub partition: Partition,
        pub generation: Generation,
        pub waiting: BTreeMap<CommandId, Waiting>,
        pub order: BTreeMap<Offset, CommandId>,
        pub next: Offset,
        pub log_now: Option<LogTime>,
        pub end_seen_at: Option<ResolverTime>,
        pub committed: Offset,
    }

    /// A partition just assigned, read again from its committed offset.
    pub fn assigned(partition: Partition, generation: Generation, committed: Offset) -> State {
        State {
            partition,
            generation,
            waiting: BTreeMap::new(),
            order: BTreeMap::new(),
            next: committed,
            log_now: None,
            end_seen_at: None,
            committed,
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub enum Event {
        Read(Offset, LogTime, Record),
        /// A poll found nothing past `next`: the end of the partition (README section 6, end offsets).
        ReachedEnd,
        LedgerReturned(CommandId, Generation, Vec<TbStatus>),
        LedgerFailed(CommandId, Generation),
        OutcomeAcknowledged(CommandId, Generation),
        OutcomeFailed(CommandId, Generation),
        TimeAdvanced,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub enum Effect {
        Submit(CommandId, Generation, Period, Vec<Transfer>),
        AppendOutcome(CommandId, Generation, Command, Outcome),
        Commit(Partition, Offset),
        Raise(Alarm),
        WakeAt(ResolverTime),
    }

    pub fn step(s: State, now: ResolverTime, e: Event, cfg: &Config) -> Step<State, Effect> {
        apply_event(s, now, e, cfg).flat_map(|s| act(s, now, cfg)).flat_map(commit)
    }

    /// A command is a straggler once a record appended the deadline plus the grace period after it has been read on
    /// its partition, or the end of an idle partition has been reached that long after it. Never by the clock alone
    /// while records remain to be read, so a resolver that restarts or lags reads past an outcome before judging.
    pub fn is_straggler(w: &Waiting, s: &State, cfg: &Config) -> bool {
        let due = w.appended + cfg.deadline + cfg.grace;
        s.log_now.is_some_and(|t| t >= due) || s.end_seen_at.is_some_and(|t| t >= due.on_resolver_clock())
    }

    fn apply_event(s: State, now: ResolverTime, e: Event, cfg: &Config) -> Step<State, Effect> {
        match e {
            Event::Read(offset, _, _) if offset.value() < s.next.value() => Step::with(s, vec![], vec![Mark::ResolverRedelivered]),
            Event::Read(offset, appended, record) => {
                let log_now = match s.log_now {
                    Some(t) if t >= appended => t,
                    _ => appended,
                };
                let mut read = State { next: offset.next(), log_now: Some(log_now), end_seen_at: None, ..s };
                match record {
                    Record::Command(command) => {
                        let id = command.id();
                        if let Entry::Vacant(slot) = read.waiting.entry(id) {
                            slot.insert(Waiting { first: offset, appended, command, phase: Phase::Idle(None) });
                            read.order.insert(offset, id);
                        }
                        Step::new(read)
                    }
                    Record::Outcome(command, _, _) => {
                        let id = command.id();
                        match read.waiting.get(&id) {
                            None => Step::new(read),
                            Some(w) => {
                                let mut marks = vec![Mark::ResolverFoundOutcome];
                                if !matches!(w.phase, Phase::Idle(_)) {
                                    marks.push(Mark::ResolverOutcomeWhileInFlight);
                                }
                                Step::with(remove(read, id), vec![], marks)
                            }
                        }
                    }
                }
            }
            Event::ReachedEnd => Step::new(State { end_seen_at: Some(now), ..s }),
            Event::LedgerReturned(id, generation, results) => match current(&s, &id, generation) {
                Some(w) if w.phase == Phase::Submitting => match rules::decide(&w.command, true, &results) {
                    Verdict::Decided(outcome, mark, alarm) => {
                        let mut effects = vec![Effect::AppendOutcome(id, generation, w.command.clone(), outcome)];
                        effects.extend(alarm.map(Effect::Raise));
                        Step::with(update(s, Waiting { phase: Phase::Appending(outcome), ..w }), effects, vec![mark])
                    }
                    Verdict::Unresolved(mark, alarm) => {
                        retry_later(s, w, now, cfg).flat_map(|r| Step::with(r, vec![Effect::Raise(alarm)], vec![mark]))
                    }
                },
                _ => stale(s),
            },
            Event::LedgerFailed(id, generation) => match current(&s, &id, generation) {
                Some(w) if w.phase == Phase::Submitting => retry_later(s, w, now, cfg),
                _ => stale(s),
            },
            Event::OutcomeAcknowledged(id, generation) => match current(&s, &id, generation) {
                Some(w) if matches!(w.phase, Phase::Appending(_)) => Step::new(remove(s, id)),
                _ => stale(s),
            },
            Event::OutcomeFailed(id, generation) => match current(&s, &id, generation) {
                Some(w) if matches!(w.phase, Phase::Appending(_)) => retry_later(s, w, now, cfg),
                _ => stale(s),
            },
            Event::TimeAdvanced => Step::new(s),
        }
    }

    /// Resolve every straggler not already in flight. Waiting commands are scanned oldest first, up to the first one
    /// that is not yet due.
    fn act(s: State, now: ResolverTime, cfg: &Config) -> Step<State, Effect> {
        let ready: Vec<Waiting> = s
            .order
            .values()
            .map(|id| &s.waiting[id])
            .take_while(|w| is_straggler(w, &s, cfg))
            .filter(|w| match w.phase {
                Phase::Idle(retry_at) => retry_at.is_none_or(|at| at <= now),
                _ => false,
            })
            .cloned()
            .collect();
        let mut acc = Step::new(s);
        for w in ready {
            acc = acc.flat_map(|s| resolve(s, w, cfg));
        }
        acc
    }

    fn resolve(s: State, w: Waiting, cfg: &Config) -> Step<State, Effect> {
        let mut marks = vec![match w.command {
            Command::Authorize(_) => Mark::ResolverFencedAuthorization,
            Command::Cancel(_) => Mark::ResolverResubmittedCancel,
            Command::Capture(..) => Mark::ResolverResubmittedCapture,
        }];
        if s.log_now.is_none_or(|t| t < w.appended + cfg.deadline + cfg.grace) {
            marks.push(Mark::ResolverJudgedIdlePartition);
        }
        let cluster = cfg.periods.period_of(w.command.key().created_at());
        let submit = Effect::Submit(w.command.id(), s.generation, cluster, transfers::for_resolver(&w.command, &cfg.ledger));
        Step::with(update(s, Waiting { phase: Phase::Submitting, ..w }), vec![submit], marks)
    }

    fn commit(s: State) -> Step<State, Effect> {
        let point = s.order.first_key_value().map_or(s.next, |(offset, _)| *offset);
        if point.value() > s.committed.value() {
            let partition = s.partition;
            Step::with(State { committed: point, ..s }, vec![Effect::Commit(partition, point)], vec![])
        } else {
            Step::new(s)
        }
    }

    fn retry_later(s: State, w: Waiting, now: ResolverTime, cfg: &Config) -> Step<State, Effect> {
        let at = now + cfg.retry_backoff;
        Step::with(update(s, Waiting { phase: Phase::Idle(Some(at)), ..w }), vec![Effect::WakeAt(at)], vec![Mark::ResolverRetryScheduled])
    }

    fn stale(s: State) -> Step<State, Effect> {
        Step::with(s, vec![], vec![Mark::ResolverStaleResult])
    }

    fn current(s: &State, id: &CommandId, generation: Generation) -> Option<Waiting> {
        if generation == s.generation { s.waiting.get(id).cloned() } else { None }
    }

    fn update(mut s: State, w: Waiting) -> State {
        s.waiting.insert(w.command.id(), w);
        s
    }

    fn remove(mut s: State, id: CommandId) -> State {
        if let Some(w) = s.waiting.remove(&id) {
            s.order.remove(&w.first);
        }
        s
    }
}

/// A call or a write for one command, on one assignment of its partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ref {
    pub partition: Partition,
    pub id: CommandId,
    pub generation: Generation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    pub partitions: BTreeMap<Partition, partition::State>,
    pub generations: Generation,
}

impl State {
    pub const INITIAL: State = State { partitions: BTreeMap::new(), generations: Generation::INITIAL };
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// Command log: what the consumer reports.
    Delivered(Delivery),
    /// Ledger: the reply to a call.
    Returned(Ref, CallResult),
    /// Command log: the reply to an outcome's write.
    Written(Ref, Written),
    /// Clock: a wake-up the resolver asked for.
    TimeAdvanced,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Effect {
    Submit(Ref, Submit),
    Append(Ref, Append),
    Commit(Commit),
    WakeAt(ResolverTime),
    Raise(Alarm),
}

pub fn machine(cfg: partition::Config) -> Machine<State, Event, Effect, ResolverTime> {
    Machine::new(State::INITIAL, move |s, now, e| step(s, now, e, &cfg))
}

pub fn step(s: State, now: ResolverTime, e: Event, cfg: &partition::Config) -> Step<State, Effect> {
    use partition::Event as P;
    let mut s = s;
    match e {
        Event::Delivered(Delivery::Assigned(p, committed)) => {
            let generation = s.generations.next();
            s.partitions.insert(p, partition::assigned(p, generation, committed));
            Step::new(State { partitions: s.partitions, generations: generation })
        }
        Event::Delivered(Delivery::Revoked(p)) if s.partitions.contains_key(&p) => {
            s.partitions.remove(&p);
            Step::with(s, vec![], vec![Mark::ResolverRevoked])
        }
        Event::Delivered(Delivery::Revoked(_)) => Step::new(s),
        Event::Delivered(Delivery::Read(p, offset, appended, record)) => on_partition(s, p, now, P::Read(offset, appended, record), cfg),
        Event::Delivered(Delivery::ReachedEnd(p)) => on_partition(s, p, now, P::ReachedEnd, cfg),
        Event::Returned(r, CallResult::Returned(statuses)) => {
            on_partition(s, r.partition, now, P::LedgerReturned(r.id, r.generation, statuses), cfg)
        }
        Event::Returned(r, CallResult::Failed) => on_partition(s, r.partition, now, P::LedgerFailed(r.id, r.generation), cfg),
        Event::Written(r, Written::Acknowledged) => on_partition(s, r.partition, now, P::OutcomeAcknowledged(r.id, r.generation), cfg),
        Event::Written(r, Written::Failed) => on_partition(s, r.partition, now, P::OutcomeFailed(r.id, r.generation), cfg),
        Event::TimeAdvanced => {
            // In partition order, as the Scala sorts the keys.
            let partitions: Vec<Partition> = s.partitions.keys().copied().collect();
            let mut acc = Step::new(s);
            for p in partitions {
                acc = acc.flat_map(|s| on_partition(s, p, now, P::TimeAdvanced, cfg));
            }
            acc
        }
    }
}

fn on_partition(s: State, p: Partition, now: ResolverTime, event: partition::Event, cfg: &partition::Config) -> Step<State, Effect> {
    let mut s = s;
    match s.partitions.remove(&p) {
        None => Step::new(s),
        Some(ps) => {
            let next = partition::step(ps, now, event, cfg);
            s.partitions.insert(p, next.state);
            Step::with(s, next.effects.into_iter().map(|f| lift(p, f)).collect(), next.marks)
        }
    }
}

fn lift(p: Partition, f: partition::Effect) -> Effect {
    use partition::Effect as F;
    match f {
        F::Submit(id, generation, cluster, transfers) => {
            Effect::Submit(Ref { partition: p, id, generation }, Submit { cluster, transfers })
        }
        F::AppendOutcome(id, generation, command, outcome) => {
            Effect::Append(Ref { partition: p, id, generation }, Append { record: Record::Outcome(command, outcome, Writer::Resolver) })
        }
        F::Commit(partition, next) => Effect::Commit(Commit { partition, next }),
        F::WakeAt(at) => Effect::WakeAt(at),
        F::Raise(alarm) => Effect::Raise(alarm),
    }
}
