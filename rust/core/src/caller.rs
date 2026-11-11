//! The caller's contracts for one client key (README section 8, who cancels; section 5, refusals). The simulated
//! caller and the stage 6 workload driver both follow this machine. What the caller may choose arrives as an event
//! (resend, capture, stop); what it must do, the machine does. Port of `Caller.scala`.
//!
//! - After a refusal, it may resend the authorization with the same key, or stop.
//! - After any non-definitive result it must cancel, resending the cancel until it is answered, and never resend the
//!   authorization.
//! - After an approval it may capture, resending the capture with the same key until it is answered.

use alloc::vec;

use crate::domain::{AuthOutcome, AuthorizeRequest, CaptureAmount, Outcome, Reply, Request};
use crate::ids::ClientKey;
use crate::machine::Step;
use crate::mark::Mark;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    AwaitAuthorization,
    Refused,
    Approved,
    Cancelling,
    Capturing(CaptureAmount),
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub authorization: AuthorizeRequest,
    pub phase: Phase,
}

impl State {
    pub fn key(&self) -> &ClientKey {
        &self.authorization.key
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    Replied(Reply),
    /// A connection error or no answer: non-definitive.
    Lost,
    Resend,
    Capture(CaptureAmount),
    Stop,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Effect {
    Send(Request),
}

pub fn start(authorization: AuthorizeRequest) -> Step<State, Effect> {
    let send = Effect::Send(Request::Authorize(authorization.clone()));
    Step::with(State { authorization, phase: Phase::AwaitAuthorization }, vec![send], vec![])
}

pub fn step(s: State, e: Event) -> Step<State, Effect> {
    use Event::*;
    use Phase::*;
    match (s.phase, e) {
        (AwaitAuthorization, Replied(Reply::Decided(Outcome::Auth(AuthOutcome::Approved)))) => Step::new(State { phase: Approved, ..s }),
        (AwaitAuthorization, Replied(Reply::Decided(Outcome::Auth(AuthOutcome::Declined(_))))) => Step::new(State { phase: Done, ..s }),
        (AwaitAuthorization, Replied(Reply::Refused(_))) => Step::new(State { phase: Refused, ..s }),
        (AwaitAuthorization, Replied(_) | Lost) => cancel(s).mark(Mark::CallerCancelsAfterNonDefinitive),

        (Refused, Resend) => {
            let send = Effect::Send(Request::Authorize(s.authorization.clone()));
            Step::with(State { phase: AwaitAuthorization, ..s }, vec![send], vec![Mark::CallerResendsRefused])
        }
        (Refused, Stop) => Step::new(State { phase: Done, ..s }),
        (Approved, Stop) => Step::with(State { phase: Done, ..s }, vec![], vec![Mark::CallerAbandonedHold]),

        (Approved, Capture(amount)) => {
            let send = Effect::Send(Request::Capture(s.key().clone(), amount));
            Step::with(State { phase: Capturing(amount), ..s }, vec![send], vec![])
        }

        (Cancelling, Replied(Reply::Decided(Outcome::Cancel(_)))) => Step::new(State { phase: Done, ..s }),
        (Cancelling, Replied(_) | Lost) => cancel(s).mark(Mark::CallerResendsCancel),

        (Capturing(_), Replied(Reply::Decided(Outcome::Capture(_)))) => Step::new(State { phase: Done, ..s }),
        (Capturing(amount), Replied(_) | Lost) => {
            let send = Effect::Send(Request::Capture(s.key().clone(), amount));
            Step::with(s, vec![send], vec![Mark::CallerResendsCapture])
        }

        _ => Step::with(s, vec![], vec![Mark::CallerIgnoredOutOfTurn]),
    }
}

fn cancel(s: State) -> Step<State, Effect> {
    let send = Effect::Send(Request::Cancel(s.key().clone()));
    Step::with(State { phase: Phase::Cancelling, ..s }, vec![send], vec![])
}
