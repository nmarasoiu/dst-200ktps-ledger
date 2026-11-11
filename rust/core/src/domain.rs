//! Requests, commands, outcomes and the records of the command log. Port of `Domain.scala`.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use crate::ids::{self, ClientKey, TransferId};
use crate::tb::TbStatus;

/// A TigerBeetle account ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct AccountId(u128);

impl AccountId {
    pub const ZERO: AccountId = AccountId(0);

    pub const fn new(value: u128) -> AccountId {
        AccountId(value)
    }

    /// The Scala `of` requires a non-negative number; here the type does.
    pub const fn of(n: u64) -> AccountId {
        AccountId(n as u128)
    }

    pub const fn value(self) -> u128 {
        self.0
    }
}

impl fmt::Debug for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:032x}", self.0)
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Authorization,
    Cancel,
    Capture,
}

/// One command's identity. Copies with the same key and kind are copies of one command: an outcome for one answers
/// them all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId {
    pub a: TransferId,
    pub kind: Kind,
}

/// A positive number of minor units. The field is private, so no partial capture of 0 or less can exist.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MinorUnits(i64);

impl MinorUnits {
    pub fn new(minor_units: i64) -> Result<MinorUnits, String> {
        if minor_units > 0 { Ok(MinorUnits(minor_units)) } else { Err(format!("a partial capture must be positive: {minor_units}")) }
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Debug for MinorUnits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A capture amount. Full and partial are separate cases and a partial amount is positive, so a raw 0 never reaches
/// TigerBeetle as a capture (README section 8, captures).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CaptureAmount {
    Full,
    Partial(MinorUnits),
}

impl CaptureAmount {
    pub fn try_partial(minor_units: i64) -> Result<CaptureAmount, String> {
        MinorUnits::new(minor_units).map(CaptureAmount::Partial)
    }

    /// Panics on an amount of 0 or less, as the Scala `Partial.apply` throws.
    pub fn partial(minor_units: i64) -> CaptureAmount {
        match Self::try_partial(minor_units) {
            Ok(amount) => amount,
            Err(e) => panic!("{e}"),
        }
    }
}

/// An authorization as a caller sends it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorizeRequest {
    pub key: ClientKey,
    pub debit: AccountId,
    pub credit: AccountId,
    pub amount: u128,
    pub ledger: i32,
}

/// What a caller sends (README section 4).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Request {
    Authorize(AuthorizeRequest),
    Cancel(ClientKey),
    Capture(ClientKey, CaptureAmount),
}

impl Request {
    pub fn authorize(key: ClientKey, debit: AccountId, credit: AccountId, amount: u128, ledger: i32) -> Request {
        Request::Authorize(AuthorizeRequest { key, debit, credit, amount, ledger })
    }

    pub fn key(&self) -> &ClientKey {
        match self {
            Request::Authorize(r) => &r.key,
            Request::Cancel(key) | Request::Capture(key, _) => key,
        }
    }

    pub fn kind(&self) -> Kind {
        match self {
            Request::Authorize(_) => Kind::Authorization,
            Request::Cancel(_) => Kind::Cancel,
            Request::Capture(..) => Kind::Capture,
        }
    }

    pub fn a(&self) -> TransferId {
        ids::authorization(self.key())
    }

    pub fn id(&self) -> CommandId {
        CommandId { a: self.a(), kind: self.kind() }
    }
}

/// An authorization as the edge logs it, with the hold timeout the edge stamped.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorizeCommand {
    pub key: ClientKey,
    pub debit: AccountId,
    pub credit: AccountId,
    pub amount: u128,
    pub ledger: i32,
    pub hold_timeout_seconds: i32,
}

/// What the edge logs: the request, with the hold timeout the edge stamped into an authorization. Every copy of the
/// command carries the stamped value, so it is part of a retry's identity (README section 8, captures). Commands
/// carry no deadline: the resolver measures it from the append time (README section 8, deadlines).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Command {
    Authorize(AuthorizeCommand),
    Cancel(ClientKey),
    Capture(ClientKey, CaptureAmount),
}

impl Command {
    pub fn stamped(request: Request, hold_timeout_seconds: i32) -> Command {
        match request {
            Request::Authorize(AuthorizeRequest { key, debit, credit, amount, ledger }) => {
                Command::Authorize(AuthorizeCommand { key, debit, credit, amount, ledger, hold_timeout_seconds })
            }
            Request::Cancel(key) => Command::Cancel(key),
            Request::Capture(key, amount) => Command::Capture(key, amount),
        }
    }

    pub fn key(&self) -> &ClientKey {
        match self {
            Command::Authorize(c) => &c.key,
            Command::Cancel(key) | Command::Capture(key, _) => key,
        }
    }

    pub fn kind(&self) -> Kind {
        match self {
            Command::Authorize(_) => Kind::Authorization,
            Command::Cancel(_) => Kind::Cancel,
            Command::Capture(..) => Kind::Capture,
        }
    }

    /// The authorization's ID, which is also the Kafka record key of every command about the hold.
    pub fn a(&self) -> TransferId {
        ids::authorization(self.key())
    }

    pub fn id(&self) -> CommandId {
        CommandId { a: self.a(), kind: self.kind() }
    }
}

/// How an authorization ended. The reason for a decline is best-effort detail (README section 5, outcomes are
/// decisions).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AuthOutcome {
    Approved,
    Declined(DeclineReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CancelOutcome {
    Cancelled,
    /// The hold was already captured.
    CancelRefused,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CaptureOutcome {
    Captured(CaptureAmount),
    CaptureRefused(CaptureRefusal),
}

/// How a command ended: the port of Scala's `Outcome`, which the three outcome enums extend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Outcome {
    Auth(AuthOutcome),
    Cancel(CancelOutcome),
    Capture(CaptureOutcome),
}

impl Outcome {
    pub fn decision(&self) -> Decision {
        match self {
            Outcome::Auth(AuthOutcome::Approved) => Decision::Approved,
            Outcome::Auth(AuthOutcome::Declined(_)) => Decision::Declined,
            Outcome::Cancel(CancelOutcome::Cancelled) => Decision::Cancelled,
            Outcome::Cancel(CancelOutcome::CancelRefused) => Decision::CancelRefused,
            Outcome::Capture(CaptureOutcome::Captured(_)) => Decision::Captured,
            Outcome::Capture(CaptureOutcome::CaptureRefused(_)) => Decision::CaptureRefused,
        }
    }
}

impl From<AuthOutcome> for Outcome {
    fn from(o: AuthOutcome) -> Outcome {
        Outcome::Auth(o)
    }
}

impl From<CancelOutcome> for Outcome {
    fn from(o: CancelOutcome) -> Outcome {
        Outcome::Cancel(o)
    }
}

impl From<CaptureOutcome> for Outcome {
    fn from(o: CaptureOutcome) -> Outcome {
        Outcome::Capture(o)
    }
}

/// An outcome without its detail: what invariants compare.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Decision {
    Approved,
    Declined,
    Cancelled,
    CancelRefused,
    Captured,
    CaptureRefused,
}

/// From generic to specific. The derived order is the Scala `Ordering`: the case first, then the status in its
/// declaration order. The merge keeps the most specific reason (README section 5, one merge).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeclineReason {
    /// Declined earlier, reason unknown: the fence found `id_already_failed`.
    Unknown,
    /// `A` is fenced, whether by expiry or by a cancel.
    NotExecuted,
    /// The key was reused with a different payload (README section 12, follow-up 6; still open).
    KeyReused(TbStatus),
    /// TigerBeetle's own reason: a transient failure, or a validation failure it did not record.
    Ledger(TbStatus),
}

/// Ordered as `DeclineReason` is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CaptureRefusal {
    /// No hold to post: a fence, no hold at all, or a resend after either.
    NotApproved,
    /// The hold was voided by a cancel.
    Cancelled,
    /// The hold was past its timeout.
    Expired,
    /// The amount exceeds the hold. Not recorded, so a corrected capture may reuse `P`.
    ExceedsHold,
    /// `P` exists with a different amount (README section 12, follow-up 6; still open).
    KeyReused(TbStatus),
    /// Another rejection by TigerBeetle.
    Ledger(TbStatus),
}

/// Why the edge refused a request before handing it to the TigerBeetle client. A refusal is definitive for the copy
/// it refuses (README section 5, refusals).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RefusalReason {
    Overload,
    NoLeader,
    CommandNotAcknowledged,
    AcknowledgedTooLate,
    PeriodNotStarted,
}

/// What the edge answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Reply {
    Decided(Outcome),
    /// An authorization the edge never sent: the caller may resend it with the same key.
    Refused(RefusalReason),
    /// An authorization without a definitive answer: the caller must cancel.
    TimedOut,
    /// A cancel or a capture without an answer: the caller resends it with the same key.
    Retry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Writer {
    Edge,
    Resolver,
}

impl Writer {
    pub const fn ordinal(self) -> u8 {
        self as u8
    }

    pub const fn from_ordinal(n: u8) -> Option<Writer> {
        match n {
            0 => Some(Writer::Edge),
            1 => Some(Writer::Resolver),
            _ => None,
        }
    }
}

/// A record of the command log, keyed by `A` (README section 4).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Record {
    Command(Command),
    Outcome(Command, Outcome, Writer),
}

impl Record {
    pub fn command(&self) -> &Command {
        match self {
            Record::Command(c) | Record::Outcome(c, _, _) => c,
        }
    }
}

/// Something an operator must look at. None of them stops the pipeline.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Alarm {
    /// A fence failed with a transient code: the fence accounts are missing or closed. `A` is burned anyway.
    FenceBurned(TbStatus),
    /// A fence met an earlier fence with different fields: the fence's fields changed (README section 5).
    FenceChanged(TbStatus),
    /// A client key reused with a different payload: a caller bug (README section 12, follow-up 6).
    KeyReused(TbStatus),
    /// Results no table row lists. No definitive outcome is given for them.
    UnexpectedResults(Kind, Vec<TbStatus>),
}
