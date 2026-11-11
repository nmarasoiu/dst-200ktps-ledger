//! Coverage marks (README section 11): a name for each row of the tables in sections 5 and 8 and for each branch
//! of a rule or a machine. A step returns the marks it hit; example tests assert them and the simulator counts
//! them across seeds. Port of `Mark.scala`; the names and their order are the Scala ones.

macro_rules! marks {
    ($( $(#[$doc:meta])* $name:ident ),* $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum Mark {
            $( $(#[$doc])* $name ),*
        }

        impl Mark {
            /// Every mark, in declaration order.
            pub const ALL: &'static [Mark] = &[ $( Mark::$name ),* ];

            /// The name, as the Scala core spells it.
            pub const fn name(self) -> &'static str {
                match self {
                    $( Mark::$name => stringify!($name) ),*
                }
            }
        }
    };
}

marks! {
    // The authorization rule: the pending transfer `A` and its result (README section 5).
    /// Created: approved.
    AuthorizationCreated,
    /// Another copy already created `A`: approved (same key twice; resending a refusal).
    AuthorizationExists,
    /// A fence holds `A`: declined, `not_executed` (paused edge; cancel overtakes; resend after the resolver's fence).
    AuthorizationFenced,
    /// An earlier copy failed transiently: declined, reason unknown (resending a refusal).
    AuthorizationIdAlreadyFailed,
    /// A transient failure: declined with TigerBeetle's reason.
    AuthorizationDeclined,
    /// Another `exists_with_different_*` code: the key was reused with a different payload.
    AuthorizationKeyReused,
    /// A validation failure, which TigerBeetle does not record.
    AuthorizationRejected,
    /// The results do not fit the command.
    AuthorizationUnresolved,

    // Replay-safe expiry: the fence for `A`, written by the resolver for a straggler, or by the edge for an
    // authorization whose period has ended (README sections 4, 5 and 8).
    /// `exists_with_different_flags`: the authorization was created, approved (edge crashed after the call).
    ExpiryFoundAuthorization,
    /// `id_already_failed`: declined earlier, reason unknown.
    ExpiryFoundDeclined,
    /// Created: `A` is fenced, `not_executed` (edge crashed before or without calling the ledger).
    ExpiryFenced,
    /// `exists`: an earlier fence holds `A`.
    ExpiryFoundFence,
    /// The fence failed with a transient code, which still burns `A`; alarm.
    ExpiryFenceBurned,
    /// Another `exists_with_different_*` code: the fence's fields changed; alarm.
    ExpiryFenceChanged,
    /// The fence was not recorded, or the result makes no sense: no outcome; alarm.
    ExpiryUnresolved,

    // Fenced cancels: the fence table of README section 8, and its replay.
    /// The authorization came first; the void released the hold.
    CancelReleasedHold,
    /// The authorization came first, but its hold had already timed out.
    CancelFoundHoldExpired,
    /// The authorization failed with a transient error; nothing was ever held.
    CancelNothingHeld,
    /// The cancel came first: the fence took `A`, and a late authorization is locked out.
    CancelFencedFirst,
    /// The authorization, then its capture: `cancel_refused`.
    CancelFoundCapture,
    /// An earlier fence holds `A`.
    CancelFoundFence,
    /// Replay: the void returns `exists`.
    CancelReplayedVoid,
    /// Replay: the void returns `id_already_failed`, because `pending_transfer_not_found` burned `V`.
    CancelReplayedNothingHeld,
    /// The fence failed with a transient code, which still burns `A`; alarm.
    CancelFenceBurned,
    /// The fence's fields changed; alarm.
    CancelFenceChanged,
    /// A combination the table does not list: no outcome; alarm.
    CancelUnresolved,

    // Captures: the capture table of README section 8.
    /// A live hold: captured.
    CaptureCreated,
    /// Its own earlier capture, resent or replayed: captured, with the same amount.
    CaptureReplayed,
    /// Its own earlier capture with a different amount.
    CaptureKeyReused,
    /// A hold voided by a cancel.
    CaptureFoundVoid,
    /// A hold past its timeout.
    CaptureFoundExpired,
    /// A hold smaller than the amount; a corrected capture may reuse `P`.
    CaptureExceedsHold,
    /// A fence instead of a hold.
    CaptureFoundFence,
    /// No hold at all; this burns `P`.
    CaptureFoundNoHold,
    /// A resend after no hold at all: `id_already_failed`.
    CaptureReplayedNoHold,
    /// Another rejection by TigerBeetle.
    CaptureRejected,
    /// A result no row lists: no outcome; alarm.
    CaptureUnresolved,

    // The edge's request flow (README sections 4, 5 and 8).
    /// An authorization whose key's period has not started: refused without writing.
    EdgeRefusedPeriodNotStarted,
    /// An authorization whose period has ended: the edge fences `A` instead.
    EdgePeriodEndedFencePath,
    /// The command log did not acknowledge the command: refused, and the ledger never called.
    EdgeRefusedCommandNotAcknowledged,
    /// The command was acknowledged too late to leave time for the ledger call and the outcome write.
    EdgeRefusedAcknowledgedTooLate,
    /// A decision answered after its outcome was acknowledged.
    EdgeAnswered,
    /// An authorization without a definitive answer: timed out, and the edge starts its own cancel.
    EdgeTimedOutAuthorization,
    /// A cancel or a capture without an answer: retry, and never a cancel.
    EdgeTimedOutRetry,
    /// The outcome was not acknowledged: answered as on a timeout.
    EdgeOutcomeNotAcknowledged,
    /// The ledger call failed without results: answered as on a timeout.
    EdgeLedgerCallFailed,
    /// A ledger result that came after the deadline, logged anyway.
    EdgeLateDecisionLogged,
    /// No ledger result by the end of the late wait; the resolver will give the outcome.
    EdgeLateResultAbandoned,

    // The edge process: its requests under admission control (README section 11, processes).
    /// The edge's own cancel found its partition without a leader: dropped, since the caller's cancel is the guarantee.
    EdgeOwnCancelDropped,
    /// A reply for a request that has ended, such as the acknowledgement of a late decision's outcome.
    EdgeReplyAfterEnd,

    // Admission control (README section 12, gap 1).
    AdmissionRefusedOverload,
    AdmissionRefusedNoLeader,
    AdmissionRetryNoLeader,
    AdmissionRetryOverload,
    AdmissionCancelDuringOverload,
    /// Past the cancels' own thresholds, a call could not come back before the deadline.
    AdmissionRetrySaturated,

    // The resolver (README section 8, resolver).
    /// A straggling authorization gets the fence, never a submission.
    ResolverFencedAuthorization,
    ResolverResubmittedCancel,
    ResolverResubmittedCapture,
    /// Judged at the end of an idle partition, by the resolver's clock.
    ResolverJudgedIdlePartition,
    /// An outcome read for a command still waiting.
    ResolverFoundOutcome,
    /// An outcome read while the resolver's own call was in flight: its result will be ignored.
    ResolverOutcomeWhileInFlight,
    /// A result for work the resolver no longer has, such as before a rebalance.
    ResolverStaleResult,
    ResolverRetryScheduled,
    ResolverRevoked,
    /// A record read again, below the next offset.
    ResolverRedelivered,

    // The caller's contracts (README section 8, who cancels).
    CallerCancelsAfterNonDefinitive,
    CallerResendsRefused,
    CallerResendsCancel,
    CallerResendsCapture,
    /// An approved hold the caller leaves to expire (README section 5, a hold is abandoned).
    CallerAbandonedHold,
    /// An event the contract does not allow in this phase, ignored.
    CallerIgnoredOutOfTurn,
}
