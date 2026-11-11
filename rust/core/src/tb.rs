//! TigerBeetle's result codes and the transfers the pipeline builds. Port of `Tb.scala`.

use alloc::vec;
use alloc::vec::Vec;
use core::fmt;

use crate::domain::{AccountId, AuthorizeCommand, CaptureAmount, Command};
use crate::ids::{self, TransferId};

macro_rules! statuses {
    ($( $name:ident = $code:expr ),* $(,)?) => {
        /// TigerBeetle's result codes for a transfer, as `CreateTransferStatus` in `src/tigerbeetle.zig` lists them,
        /// with their numeric values. A closed type: every rule matches it exhaustively through `group`. The order
        /// is the Scala enum's, so the derived order matches its ordinals.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum TbStatus {
            $( $name ),*
        }

        impl TbStatus {
            /// Every status, in declaration order.
            pub const ALL: &'static [TbStatus] = &[ $( TbStatus::$name ),* ];

            /// TigerBeetle's numeric code.
            pub const fn code(self) -> u32 {
                match self {
                    $( TbStatus::$name => $code ),*
                }
            }

            /// The name, as the Scala core spells it and as the record codec writes it.
            pub const fn name(self) -> &'static str {
                match self {
                    $( TbStatus::$name => stringify!($name) ),*
                }
            }

            pub fn from_name(name: &str) -> Option<TbStatus> {
                match name {
                    $( stringify!($name) => Some(TbStatus::$name), )*
                    _ => None,
                }
            }
        }
    };
}

statuses! {
    Created = 0xffff_ffff,
    DeprecatedOk = 0,
    LinkedEventFailed = 1,
    LinkedEventChainOpen = 2,
    ImportedEventExpected = 56,
    ImportedEventNotExpected = 57,
    TimestampMustBeZero = 3,
    ImportedEventTimestampOutOfRange = 58,
    ImportedEventTimestampMustNotAdvance = 59,
    ReservedFlag = 4,
    IdMustNotBeZero = 5,
    IdMustNotBeIntMax = 6,
    ExistsWithDifferentFlags = 36,
    ExistsWithDifferentPendingId = 40,
    ExistsWithDifferentTimeout = 44,
    ExistsWithDifferentDebitAccountId = 37,
    ExistsWithDifferentCreditAccountId = 38,
    ExistsWithDifferentAmount = 39,
    ExistsWithDifferentUserData128 = 41,
    ExistsWithDifferentUserData64 = 42,
    ExistsWithDifferentUserData32 = 43,
    ExistsWithDifferentLedger = 67,
    ExistsWithDifferentCode = 45,
    Exists = 46,
    IdAlreadyFailed = 68,
    FlagsAreMutuallyExclusive = 7,
    DebitAccountIdMustNotBeZero = 8,
    DebitAccountIdMustNotBeIntMax = 9,
    CreditAccountIdMustNotBeZero = 10,
    CreditAccountIdMustNotBeIntMax = 11,
    AccountsMustBeDifferent = 12,
    PendingIdMustBeZero = 13,
    PendingIdMustNotBeZero = 14,
    PendingIdMustNotBeIntMax = 15,
    PendingIdMustBeDifferent = 16,
    TimeoutReservedForPendingTransfer = 17,
    ClosingTransferMustBePending = 64,
    LedgerMustNotBeZero = 19,
    CodeMustNotBeZero = 20,
    DebitAccountNotFound = 21,
    CreditAccountNotFound = 22,
    AccountsMustHaveTheSameLedger = 23,
    TransferMustHaveTheSameLedgerAsAccounts = 24,
    PendingTransferNotFound = 25,
    PendingTransferNotPending = 26,
    PendingTransferHasDifferentDebitAccountId = 27,
    PendingTransferHasDifferentCreditAccountId = 28,
    PendingTransferHasDifferentLedger = 29,
    PendingTransferHasDifferentCode = 30,
    ExceedsPendingTransferAmount = 31,
    PendingTransferHasDifferentAmount = 32,
    PendingTransferAlreadyPosted = 33,
    PendingTransferAlreadyVoided = 34,
    PendingTransferExpired = 35,
    ImportedEventTimestampMustNotRegress = 60,
    ImportedEventTimestampMustPostdateDebitAccount = 61,
    ImportedEventTimestampMustPostdateCreditAccount = 62,
    ImportedEventTimeoutMustBeZero = 63,
    DebitAccountAlreadyClosed = 65,
    CreditAccountAlreadyClosed = 66,
    OverflowsDebitsPending = 47,
    OverflowsCreditsPending = 48,
    OverflowsDebitsPosted = 49,
    OverflowsCreditsPosted = 50,
    OverflowsDebits = 51,
    OverflowsCredits = 52,
    OverflowsTimeout = 53,
    ExceedsCredits = 54,
    ExceedsDebits = 55,
    Deprecated18 = 18,
}

impl TbStatus {
    /// What the code means to the rules. No wildcard: a new code fails the build here until it is placed.
    pub const fn group(self) -> TbGroup {
        use TbStatus::*;
        match self {
            Created => TbGroup::Created,
            Exists => TbGroup::Exists,
            ExistsWithDifferentFlags => TbGroup::ExistsWithDifferentFlags,
            ExistsWithDifferentPendingId
            | ExistsWithDifferentTimeout
            | ExistsWithDifferentDebitAccountId
            | ExistsWithDifferentCreditAccountId
            | ExistsWithDifferentAmount
            | ExistsWithDifferentUserData128
            | ExistsWithDifferentUserData64
            | ExistsWithDifferentUserData32
            | ExistsWithDifferentLedger
            | ExistsWithDifferentCode => TbGroup::ExistsWithDifferentField(self),
            IdAlreadyFailed => TbGroup::IdAlreadyFailed,
            // Exactly these seven are transient and burn the ID (README section 6, `CreateTransferStatus.transient`).
            DebitAccountNotFound
            | CreditAccountNotFound
            | PendingTransferNotFound
            | ExceedsCredits
            | ExceedsDebits
            | DebitAccountAlreadyClosed
            | CreditAccountAlreadyClosed => TbGroup::Transient(self),
            PendingTransferNotPending => TbGroup::PendingNotPending,
            PendingTransferAlreadyPosted => TbGroup::AlreadyPosted,
            PendingTransferAlreadyVoided => TbGroup::AlreadyVoided,
            PendingTransferExpired => TbGroup::PendingExpired,
            ExceedsPendingTransferAmount => TbGroup::ExceedsPendingAmount,
            DeprecatedOk
            | LinkedEventFailed
            | LinkedEventChainOpen
            | ImportedEventExpected
            | ImportedEventNotExpected
            | TimestampMustBeZero
            | ImportedEventTimestampOutOfRange
            | ImportedEventTimestampMustNotAdvance
            | ReservedFlag
            | IdMustNotBeZero
            | IdMustNotBeIntMax
            | FlagsAreMutuallyExclusive
            | DebitAccountIdMustNotBeZero
            | DebitAccountIdMustNotBeIntMax
            | CreditAccountIdMustNotBeZero
            | CreditAccountIdMustNotBeIntMax
            | AccountsMustBeDifferent
            | PendingIdMustBeZero
            | PendingIdMustNotBeZero
            | PendingIdMustNotBeIntMax
            | PendingIdMustBeDifferent
            | TimeoutReservedForPendingTransfer
            | ClosingTransferMustBePending
            | LedgerMustNotBeZero
            | CodeMustNotBeZero
            | AccountsMustHaveTheSameLedger
            | TransferMustHaveTheSameLedgerAsAccounts
            | PendingTransferHasDifferentDebitAccountId
            | PendingTransferHasDifferentCreditAccountId
            | PendingTransferHasDifferentLedger
            | PendingTransferHasDifferentCode
            | PendingTransferHasDifferentAmount
            | ImportedEventTimestampMustNotRegress
            | ImportedEventTimestampMustPostdateDebitAccount
            | ImportedEventTimestampMustPostdateCreditAccount
            | ImportedEventTimeoutMustBeZero
            | OverflowsDebitsPending
            | OverflowsCreditsPending
            | OverflowsDebitsPosted
            | OverflowsCreditsPosted
            | OverflowsDebits
            | OverflowsCredits
            | OverflowsTimeout
            | Deprecated18 => TbGroup::Rejected(self),
        }
    }
}

/// Result codes grouped by what they tell the rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TbGroup {
    Created,
    Exists,
    /// Only a fence can cause this on an authorization: flags are compared first (README section 5).
    ExistsWithDifferentFlags,
    ExistsWithDifferentField(TbStatus),
    IdAlreadyFailed,
    /// One of the seven codes that burn the ID: it can never succeed again.
    Transient(TbStatus),
    PendingNotPending,
    AlreadyPosted,
    AlreadyVoided,
    PendingExpired,
    /// Not recorded, so a corrected request may reuse the ID.
    ExceedsPendingAmount,
    /// A validation failure, not recorded: the ID stays free.
    Rejected(TbStatus),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransferFlag {
    Pending,
    PostPendingTransfer,
    VoidPendingTransfer,
}

impl TransferFlag {
    const ALL: [TransferFlag; 3] = [TransferFlag::Pending, TransferFlag::PostPendingTransfer, TransferFlag::VoidPendingTransfer];

    const fn bit(self) -> u8 {
        1 << (self as u8)
    }
}

/// A set of transfer flags, the port of Scala's `Set[TransferFlag]`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TransferFlags(u8);

impl TransferFlags {
    pub const NONE: TransferFlags = TransferFlags(0);

    pub const fn of(flag: TransferFlag) -> TransferFlags {
        TransferFlags(flag.bit())
    }

    pub const fn contains(self, flag: TransferFlag) -> bool {
        self.0 & flag.bit() != 0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The flags in declaration order.
    pub fn iter(self) -> impl Iterator<Item = TransferFlag> {
        TransferFlag::ALL.into_iter().filter(move |f| self.contains(*f))
    }

    pub fn first(self) -> Option<TransferFlag> {
        self.iter().next()
    }
}

impl fmt::Debug for TransferFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

/// The fields of a TigerBeetle transfer the pipeline sets. User data stays 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Transfer {
    pub id: TransferId,
    pub debit: AccountId,
    pub credit: AccountId,
    pub amount: u128,
    pub pending_id: TransferId,
    pub ledger: i32,
    pub code: i32,
    pub flags: TransferFlags,
    pub timeout_seconds: i32,
}

impl Transfer {
    pub const fn is_pending(&self) -> bool {
        self.flags.contains(TransferFlag::Pending)
    }

    pub const fn is_post(&self) -> bool {
        self.flags.contains(TransferFlag::PostPendingTransfer)
    }

    pub const fn is_void(&self) -> bool {
        self.flags.contains(TransferFlag::VoidPendingTransfer)
    }
}

/// What the edge and the resolver put into the transfers they build. The fence's fields are fixed for all time,
/// like the ID layout: every fence is identical, so a fence that meets an earlier one returns `exists` (README
/// section 8, cancels).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LedgerConfig {
    pub fence_debit: AccountId,
    pub fence_credit: AccountId,
    pub fence_ledger: i32,
    pub fence_code: i32,
    pub authorization_code: i32,
}

/// The transfers each command becomes. Port of the `Transfers` object.
pub mod transfers {
    use super::*;

    /// A pending transfer with ID `A` and the hold timeout the edge stamped.
    pub fn authorization(c: &AuthorizeCommand, cfg: &LedgerConfig) -> Transfer {
        Transfer {
            id: ids::authorization(&c.key),
            debit: c.debit,
            credit: c.credit,
            amount: c.amount,
            pending_id: TransferId::ZERO,
            ledger: c.ledger,
            code: cfg.authorization_code,
            flags: TransferFlags::of(TransferFlag::Pending),
            timeout_seconds: c.hold_timeout_seconds,
        }
    }

    /// ID `A`, amount 0, no flags, between the two fence accounts (README section 8, fencing).
    pub fn fence(a: TransferId, cfg: &LedgerConfig) -> Transfer {
        Transfer {
            id: a,
            debit: cfg.fence_debit,
            credit: cfg.fence_credit,
            amount: 0,
            pending_id: TransferId::ZERO,
            ledger: cfg.fence_ledger,
            code: cfg.fence_code,
            flags: TransferFlags::NONE,
            timeout_seconds: 0,
        }
    }

    /// ID `V`, amount 0 for the full pending amount; accounts, ledger and code inherit from the hold.
    pub fn void(a: TransferId) -> Transfer {
        Transfer {
            id: ids::void(a),
            debit: AccountId::ZERO,
            credit: AccountId::ZERO,
            amount: 0,
            pending_id: a,
            ledger: 0,
            code: 0,
            flags: TransferFlags::of(TransferFlag::VoidPendingTransfer),
            timeout_seconds: 0,
        }
    }

    /// ID `P`. Full is sent as the maximum amount, since a post of 0 posts nothing (README section 8, captures).
    pub fn post(a: TransferId, amount: CaptureAmount) -> Transfer {
        let value = match amount {
            CaptureAmount::Full => u128::MAX,
            CaptureAmount::Partial(n) => n.get() as u128,
        };
        Transfer {
            id: ids::post(a),
            debit: AccountId::ZERO,
            credit: AccountId::ZERO,
            amount: value,
            pending_id: a,
            ledger: 0,
            code: 0,
            flags: TransferFlags::of(TransferFlag::PostPendingTransfer),
            timeout_seconds: 0,
        }
    }

    /// A cancel: one batch of two unlinked events, the fence first.
    pub fn cancel(a: TransferId, cfg: &LedgerConfig) -> Vec<Transfer> {
        vec![fence(a, cfg), void(a)]
    }

    /// What the edge sends. An authorization whose period has ended gets the fence instead (README section 4).
    pub fn for_edge(command: &Command, fence_path: bool, cfg: &LedgerConfig) -> Vec<Transfer> {
        match command {
            Command::Authorize(_) if fence_path => vec![fence(command.a(), cfg)],
            Command::Authorize(c) => vec![authorization(c, cfg)],
            Command::Cancel(_) => cancel(command.a(), cfg),
            Command::Capture(_, amount) => vec![post(command.a(), *amount)],
        }
    }

    /// What the resolver sends. No branch builds an authorization: the resolver fences it (README section 8).
    pub fn for_resolver(command: &Command, cfg: &LedgerConfig) -> Vec<Transfer> {
        match command {
            Command::Authorize(_) => vec![fence(command.a(), cfg)],
            Command::Cancel(_) => cancel(command.a(), cfg),
            Command::Capture(_, amount) => vec![post(command.a(), *amount)],
        }
    }
}
