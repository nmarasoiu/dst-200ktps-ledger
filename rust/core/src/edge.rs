//! An edge instance (README section 4): every request it holds, each its own `edge_request` machine, under admission
//! control (README section 12, gap 1). It speaks the ports. Requests arrive and are answered through the caller API,
//! and each request's writes, ledger call and wake-ups carry its `Ref`, which the reply carries back. It holds nothing
//! a restart must restore (README section 8, failover): a crash drops every request. Port of `Edge.scala`.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec;
use alloc::vec::Vec;

use crate::admission;
use crate::domain::{Alarm, Record, Reply, Request, Writer};
use crate::edge_request::{self, Origin};
use crate::machine::{EdgeTime, Machine, Span, Step, WallTime};
use crate::mark::Mark;
use crate::ports::Connection;
use crate::ports::command_log::{Append, Leaders, Written};
use crate::ports::ledger::{CallResult, Submit};

#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub request: edge_request::Config,
    pub admission: admission::Config,
}

/// The edge's two clocks, read at each step: the monotonic one times deadlines, and the wall one places a key's
/// period (README section 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Now {
    pub monotonic: EdgeTime,
    pub wall: WallTime,
}

/// A request on this instance: one a caller sent, named by its connection, or the edge's own cancel of one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Ref {
    Caller(Connection),
    OwnCancel(Connection),
}

/// A request in flight. `called_at` is when its ledger call went out, while that call has no reply.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InFlight {
    pub request: edge_request::State,
    pub called_at: Option<EdgeTime>,
}

/// Admission control counts every request in `requests`, until it ends, and every call in `abandoned`, until it
/// replies: a request can stop waiting, but a call handed to the client cannot be taken back, so it still takes the
/// ledger's time.
#[derive(Clone, Debug, PartialEq)]
pub struct State {
    pub requests: BTreeMap<Ref, InFlight>,
    pub admission: admission::State,
    pub abandoned: BTreeSet<Ref>,
}

impl State {
    pub fn initial(cfg: &Config) -> State {
        State { requests: BTreeMap::new(), admission: admission::State::initial(&cfg.admission), abandoned: BTreeSet::new() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// Caller API: a request arrived on a new connection.
    Arrived(Connection, Request),
    /// Command log: the reply to a write. The record tells a command's write from its outcome's.
    Written(Ref, Record, Written),
    /// Command log: the producer's metadata changed.
    LeadersChanged(Leaders),
    /// Ledger: the reply to a request's call.
    Returned(Ref, CallResult),
    /// Clock: a wake-up a request asked for.
    Woke(Ref),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Effect {
    Answer(Connection, Reply),
    Append(Ref, Append),
    Submit(Ref, Submit),
    WakeAt(Ref, EdgeTime),
    Raise(Alarm),
}

pub fn machine(cfg: Config) -> Machine<State, Event, Effect, Now> {
    Machine::new(State::initial(&cfg), move |s, now, e| step(s, now, e, &cfg))
}

pub fn step(s: State, now: Now, e: Event, cfg: &Config) -> Step<State, Effect> {
    use edge_request::Event as R;
    let mut s = s;
    match e {
        Event::Arrived(connection, request) => admit(s, Ref::Caller(connection), request, now, cfg),
        Event::Written(r, record, result) => {
            let event = match (record, result) {
                (Record::Command(_), Written::Acknowledged) => R::CommandAcknowledged,
                (Record::Command(_), Written::Failed) => R::CommandFailed,
                (Record::Outcome(..), Written::Acknowledged) => R::OutcomeAcknowledged,
                (Record::Outcome(..), Written::Failed) => R::OutcomeFailed,
            };
            // Taken out of the map here and placed back by `run`, as the Scala `updated` would leave it.
            match s.requests.remove(&r) {
                Some(in_flight) => advance(s, r, in_flight, event, now, cfg),
                None => reply_after_end(s),
            }
        }
        Event::Returned(r, result) => {
            let event = match result {
                CallResult::Returned(statuses) => R::LedgerReturned(statuses),
                CallResult::Failed => R::LedgerFailed,
            };
            match s.requests.remove(&r) {
                Some(in_flight) => {
                    // The ledger's latency runs from the call to its reply.
                    if let Some(at) = in_flight.called_at {
                        s.admission = latency(s.admission, now.monotonic - at, cfg);
                    }
                    advance(s, r, InFlight { called_at: None, ..in_flight }, event, now, cfg)
                }
                None if s.abandoned.contains(&r) => {
                    s.admission = admission::step(s.admission, admission::Event::Finished, &cfg.admission).state;
                    s.abandoned.remove(&r);
                    reply_after_end(s)
                }
                None => reply_after_end(s),
            }
        }
        Event::LeadersChanged(leaders) => {
            s.admission = admission::step(s.admission, admission::Event::LeadersChanged(leaders.leaderless), &cfg.admission).state;
            Step::new(s)
        }
        // Most wake-ups come after their request has ended, and change nothing.
        Event::Woke(r) => match s.requests.remove(&r) {
            Some(in_flight) => advance(s, r, in_flight, R::TimeAdvanced, now, cfg),
            None => Step::new(s),
        },
    }
}

/// Admission control decides first. A refusal or a retry is answered at once, with nothing written or sent. The
/// edge's own cancel passes admission control like any cancel: admitted under overload, and dropped when its
/// partition has no leader, since the caller's cancel is the guarantee (README section 8, who cancels).
fn admit(s: State, r: Ref, request: Request, now: Now, cfg: &Config) -> Step<State, Effect> {
    let mut s = s;
    let decided = admission::step(s.admission, admission::Event::Arrived(request.clone()), &cfg.admission);
    s.admission = decided.state;
    let mut acc = Step::with(s, vec![], decided.marks);
    for d in decided.effects {
        acc = acc.flat_map(|st| match d {
            admission::Decision::Admit => {
                let origin = match r {
                    Ref::Caller(_) => Origin::Caller,
                    Ref::OwnCancel(_) => Origin::Edge,
                };
                let started = edge_request::start(request.clone(), origin, now.monotonic, now.wall, &cfg.request);
                run(st, r, None, started, now, cfg)
            }
            admission::Decision::Refuse(reason) => Step::with(st, answer(r, Reply::Refused(reason)), vec![]),
            admission::Decision::Retry => match r {
                Ref::Caller(_) => Step::with(st, answer(r, Reply::Retry), vec![]),
                Ref::OwnCancel(_) => Step::with(st, vec![], vec![Mark::EdgeOwnCancelDropped]),
            },
        });
    }
    acc
}

fn advance(s: State, r: Ref, in_flight: InFlight, e: edge_request::Event, now: Now, cfg: &Config) -> Step<State, Effect> {
    let stepped = edge_request::step(in_flight.request, now.monotonic, e, &cfg.request);
    run(s, r, in_flight.called_at, stepped, now, cfg)
}

/// Places a request's new state, then carries out its effects. A request that ends leaves admission control's count,
/// unless its ledger call still has no reply. Then the call stays counted until it replies, and the time it waited
/// counts as a latency sample, so a ledger that stops answering still lowers the limit.
fn run(
    s: State,
    r: Ref,
    called_at: Option<EdgeTime>,
    step: Step<edge_request::State, edge_request::Effect>,
    now: Now,
    cfg: &Config,
) -> Step<State, Effect> {
    let called = if step.effects.iter().any(|f| matches!(f, edge_request::Effect::Submit(..))) { Some(now.monotonic) } else { called_at };
    let Step { state: request, effects, marks } = step;
    let mut placed = s;
    if request.phase == edge_request::Phase::Done {
        placed.requests.remove(&r);
        match called {
            Some(at) => {
                placed.admission = latency(placed.admission, now.monotonic - at, cfg);
                placed.abandoned.insert(r);
            }
            None => placed.admission = admission::step(placed.admission, admission::Event::Finished, &cfg.admission).state,
        }
    } else {
        placed.requests.insert(r, InFlight { request, called_at: called });
    }
    let mut acc = Step::with(placed, vec![], marks);
    for f in effects {
        acc = acc.flat_map(|st| carry_out(st, r, f, now, cfg));
    }
    acc
}

fn carry_out(s: State, r: Ref, f: edge_request::Effect, now: Now, cfg: &Config) -> Step<State, Effect> {
    use edge_request::Effect as F;
    match f {
        F::AppendCommand(command) => Step::with(s, vec![Effect::Append(r, Append { record: Record::Command(command) })], vec![]),
        F::Submit(cluster, transfers) => Step::with(s, vec![Effect::Submit(r, Submit { cluster, transfers })], vec![]),
        F::AppendOutcome(command, outcome) => {
            Step::with(s, vec![Effect::Append(r, Append { record: Record::Outcome(command, outcome, Writer::Edge) })], vec![])
        }
        F::Answer(reply) => Step::with(s, answer(r, reply), vec![]),
        F::StartCancel(key) => match r {
            Ref::Caller(connection) => admit(s, Ref::OwnCancel(connection), Request::Cancel(key), now, cfg),
            // Only an authorization a caller sent starts the edge's own cancel.
            Ref::OwnCancel(_) => Step::new(s),
        },
        F::WakeAt(at) => Step::with(s, vec![Effect::WakeAt(r, at)], vec![]),
        F::Raise(alarm) => Step::with(s, vec![Effect::Raise(alarm)], vec![]),
    }
}

/// The edge's own cancel answers no one.
fn answer(r: Ref, reply: Reply) -> Vec<Effect> {
    match r {
        Ref::Caller(connection) => vec![Effect::Answer(connection, reply)],
        Ref::OwnCancel(_) => vec![],
    }
}

fn latency(a: admission::State, waited: Span, cfg: &Config) -> admission::State {
    admission::step(a, admission::Event::LedgerLatency(waited), &cfg.admission).state
}

fn reply_after_end(s: State) -> Step<State, Effect> {
    Step::with(s, vec![], vec![Mark::EdgeReplyAfterEnd])
}
