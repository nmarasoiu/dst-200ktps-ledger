//! The edge's flow for one request (README section 4): log the command, call the ledger, log the outcome, answer,
//! all within the deadline. Each request is its own machine, so requests share no state but admission control.
//! Port of `EdgeRequest.scala`.

use alloc::vec;
use alloc::vec::Vec;

use crate::domain::{Alarm, Command, Kind, Outcome, RefusalReason, Reply, Request};
use crate::ids::ClientKey;
use crate::machine::{EdgeTime, Span, Step, WallTime};
use crate::mark::Mark;
use crate::periods::{Period, PeriodSchedule};
use crate::rules::{self, Verdict};
use crate::tb::{LedgerConfig, TbStatus, Transfer, transfers};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// The 1.5 s deadline, on the edge's monotonic clock.
    pub deadline: Span,
    /// The last moment, from arrival, to hand an authorization to the TigerBeetle client, leaving time for the ledger
    /// call and the outcome write (README section 8, step budgets).
    pub hand_off_budget: Span,
    /// How long after the deadline the edge still waits for a ledger result, to log it.
    pub late_result_wait: Span,
    /// The hold timeout stamped into each authorization: 7 days.
    pub hold_timeout_seconds: i32,
    pub periods: PeriodSchedule,
    pub ledger: LedgerConfig,
}

impl Config {
    /// Panics unless the hand-off budget leaves time before the deadline, as the Scala `require` throws.
    pub fn new(
        deadline: Span,
        hand_off_budget: Span,
        late_result_wait: Span,
        hold_timeout_seconds: i32,
        periods: PeriodSchedule,
        ledger: LedgerConfig,
    ) -> Config {
        assert!(hand_off_budget < deadline, "the hand-off budget {hand_off_budget} must leave time before the deadline {deadline}");
        Config { deadline, hand_off_budget, late_result_wait, hold_timeout_seconds, periods, ledger }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Origin {
    Caller,
    /// The edge's own cancel after an authorization timed out (README section 8, who cancels). It answers no one.
    Edge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    AwaitCommandAck,
    /// Handed to the TigerBeetle client: refusing is no longer possible.
    AwaitLedger,
    AwaitOutcomeAck(Outcome),
    /// Answered without a decision; waiting for the ledger result only to log it.
    AwaitLateLedger,
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub command: Command,
    pub origin: Origin,
    pub start: EdgeTime,
    pub cluster: Period,
    pub fence_path: bool,
    pub phase: Phase,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    CommandAcknowledged,
    CommandFailed,
    LedgerReturned(Vec<TbStatus>),
    /// The call failed without results, so its effect is unknown.
    LedgerFailed,
    OutcomeAcknowledged,
    OutcomeFailed,
    TimeAdvanced,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Effect {
    AppendCommand(Command),
    /// One call to the cluster of the key's period, through the client the process shares.
    Submit(Period, Vec<Transfer>),
    AppendOutcome(Command, Outcome),
    Answer(Reply),
    /// Start the edge's own cancel: a new request with origin `Edge`.
    StartCancel(ClientKey),
    WakeAt(EdgeTime),
    Raise(Alarm),
}

/// A request admitted by admission control. `wall` is the edge's wall clock, read only to place the key's period
/// (README section 4).
pub fn start(request: Request, origin: Origin, now: EdgeTime, wall: WallTime, cfg: &Config) -> Step<State, Effect> {
    let command = Command::stamped(request, cfg.hold_timeout_seconds);
    let cluster = cfg.periods.period_of(command.key().created_at());
    let is_authorization = command.kind() == Kind::Authorization;
    let s = State { command, origin, start: now, cluster, fence_path: false, phase: Phase::AwaitCommandAck };
    let write = vec![Effect::AppendCommand(s.command.clone()), Effect::WakeAt(hand_off_by(&s, cfg))];
    if is_authorization && !cfg.periods.has_started(cluster, wall) {
        // Nothing is written or sent, so the refusal is safe whatever the clocks.
        let answer = answer(&s, Reply::Refused(RefusalReason::PeriodNotStarted));
        finish(s, answer, &[Mark::EdgeRefusedPeriodNotStarted])
    } else if is_authorization && cfg.periods.has_ended(cluster, wall) {
        Step::with(State { fence_path: true, ..s }, write, vec![Mark::EdgePeriodEndedFencePath])
    } else {
        Step::with(s, write, vec![])
    }
}

pub fn step(s: State, now: EdgeTime, e: Event, cfg: &Config) -> Step<State, Effect> {
    use Event::*;
    use Phase::*;
    let deadline = s.start + cfg.deadline;
    let hand_off_by = hand_off_by(&s, cfg);
    match (s.phase, e) {
        (AwaitCommandAck, CommandAcknowledged) if now >= hand_off_by => {
            refuse(s, RefusalReason::AcknowledgedTooLate, Mark::EdgeRefusedAcknowledgedTooLate)
        }
        (AwaitCommandAck, CommandAcknowledged) => {
            let submit = Effect::Submit(s.cluster, transfers::for_edge(&s.command, s.fence_path, &cfg.ledger));
            Step::with(State { phase: AwaitLedger, ..s }, vec![submit, Effect::WakeAt(deadline)], vec![])
        }
        (AwaitCommandAck, CommandFailed) => refuse(s, RefusalReason::CommandNotAcknowledged, Mark::EdgeRefusedCommandNotAcknowledged),
        (AwaitCommandAck, TimeAdvanced) if now >= hand_off_by => {
            refuse(s, RefusalReason::AcknowledgedTooLate, Mark::EdgeRefusedAcknowledgedTooLate)
        }

        (AwaitLedger, LedgerReturned(results)) => match rules::decide(&s.command, s.fence_path, &results) {
            Verdict::Decided(outcome, mark, alarm) if now < deadline => {
                let mut effects = vec![Effect::AppendOutcome(s.command.clone(), outcome)];
                effects.extend(raise(alarm));
                Step::with(State { phase: AwaitOutcomeAck(outcome), ..s }, effects, vec![mark])
            }
            Verdict::Decided(outcome, mark, alarm) => {
                // Past the deadline: answer as on a timeout, but still log the decision (README section 4).
                let command = s.command.clone();
                timed_out(s).flat_map(|done| {
                    let mut effects = vec![Effect::AppendOutcome(command, outcome)];
                    effects.extend(raise(alarm));
                    Step::with(done, effects, vec![mark, Mark::EdgeLateDecisionLogged])
                })
            }
            Verdict::Unresolved(mark, alarm) => timed_out(s).flat_map(|done| Step::with(done, vec![Effect::Raise(alarm)], vec![mark])),
        },
        (AwaitLedger, LedgerFailed) => timed_out(s).mark(Mark::EdgeLedgerCallFailed),
        (AwaitLedger, TimeAdvanced) if now >= deadline => timed_out(s).flat_map(|done| {
            let wake = Effect::WakeAt(deadline + cfg.late_result_wait);
            Step::with(State { phase: AwaitLateLedger, ..done }, vec![wake], vec![])
        }),

        (AwaitOutcomeAck(outcome), OutcomeAcknowledged) if now < deadline => {
            let answer = answer(&s, Reply::Decided(outcome));
            finish(s, answer, &[Mark::EdgeAnswered])
        }
        (AwaitOutcomeAck(_), OutcomeFailed) => timed_out(s).mark(Mark::EdgeOutcomeNotAcknowledged),
        (AwaitOutcomeAck(_), OutcomeAcknowledged) => timed_out(s),
        (AwaitOutcomeAck(_), TimeAdvanced) if now >= deadline => timed_out(s),

        (AwaitLateLedger, LedgerReturned(results)) => match rules::decide(&s.command, s.fence_path, &results) {
            Verdict::Decided(outcome, mark, alarm) => {
                let mut effects = vec![Effect::AppendOutcome(s.command.clone(), outcome)];
                effects.extend(raise(alarm));
                Step::with(State { phase: Done, ..s }, effects, vec![mark, Mark::EdgeLateDecisionLogged])
            }
            Verdict::Unresolved(mark, alarm) => finish(s, vec![Effect::Raise(alarm)], &[mark]),
        },
        (AwaitLateLedger, LedgerFailed) => finish(s, vec![], &[Mark::EdgeLedgerCallFailed]),
        (AwaitLateLedger, TimeAdvanced) if now >= deadline + cfg.late_result_wait => finish(s, vec![], &[Mark::EdgeLateResultAbandoned]),

        // Late, duplicate or out-of-phase events change nothing.
        _ => Step::new(s),
    }
}

/// An authorization must be handed to the TigerBeetle client by its budget; a cancel or a capture by the deadline.
pub fn hand_off_by(s: &State, cfg: &Config) -> EdgeTime {
    if s.command.kind() == Kind::Authorization { s.start + cfg.hand_off_budget } else { s.start + cfg.deadline }
}

/// A refusal for an authorization, which the caller may resend with the same key; retry for a cancel or a capture.
fn refuse(s: State, reason: RefusalReason, mark: Mark) -> Step<State, Effect> {
    let reply = if s.command.kind() == Kind::Authorization { Reply::Refused(reason) } else { Reply::Retry };
    let answer = answer(&s, reply);
    finish(s, answer, &[mark])
}

/// No definitive answer: an authorization times out and the edge cancels it; a cancel or a capture is retried.
fn timed_out(s: State) -> Step<State, Effect> {
    match &s.command {
        Command::Authorize(c) => {
            let mut effects = answer(&s, Reply::TimedOut);
            if s.origin == Origin::Caller {
                effects.push(Effect::StartCancel(c.key.clone()));
            }
            finish(s, effects, &[Mark::EdgeTimedOutAuthorization])
        }
        _ => {
            let effects = answer(&s, Reply::Retry);
            finish(s, effects, &[Mark::EdgeTimedOutRetry])
        }
    }
}

fn finish(s: State, effects: Vec<Effect>, marks: &[Mark]) -> Step<State, Effect> {
    Step::with(State { phase: Phase::Done, ..s }, effects, marks.to_vec())
}

fn answer(s: &State, reply: Reply) -> Vec<Effect> {
    if s.origin == Origin::Caller { vec![Effect::Answer(reply)] } else { vec![] }
}

fn raise(alarm: Option<Alarm>) -> Option<Effect> {
    alarm.map(Effect::Raise)
}
