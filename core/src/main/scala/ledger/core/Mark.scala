package ledger.core

/** Coverage marks (README section 11): a name for each row of the tables in sections 5 and 8 and for each branch
  * of a rule or a machine. A step returns the marks it hit; example tests assert them and the simulator counts
  * them across seeds.
  */
enum Mark derives CanEqual:
  // The authorization rule: the pending transfer `A` and its result (README section 5).
  /** Created: approved. */
  case AuthorizationCreated
  /** Another copy already created `A`: approved (same key twice; resending a refusal). */
  case AuthorizationExists
  /** A fence holds `A`: declined, `not_executed` (paused edge; cancel overtakes; resend after the resolver's fence). */
  case AuthorizationFenced
  /** An earlier copy failed transiently: declined, reason unknown (resending a refusal). */
  case AuthorizationIdAlreadyFailed
  /** A transient failure: declined with TigerBeetle's reason. */
  case AuthorizationDeclined
  /** Another `exists_with_different_*` code: the key was reused with a different payload. */
  case AuthorizationKeyReused
  /** A validation failure, which TigerBeetle does not record. */
  case AuthorizationRejected
  /** The results do not fit the command. */
  case AuthorizationUnresolved

  // Replay-safe expiry: the fence for `A`, written by the resolver for a straggler, or by the edge for an
  // authorization whose period has ended (README sections 4, 5 and 8).
  /** `exists_with_different_flags`: the authorization was created, approved (edge crashed after the call). */
  case ExpiryFoundAuthorization
  /** `id_already_failed`: declined earlier, reason unknown. */
  case ExpiryFoundDeclined
  /** Created: `A` is fenced, `not_executed` (edge crashed before or without calling the ledger). */
  case ExpiryFenced
  /** `exists`: an earlier fence holds `A`. */
  case ExpiryFoundFence
  /** The fence failed with a transient code, which still burns `A`; alarm. */
  case ExpiryFenceBurned
  /** Another `exists_with_different_*` code: the fence's fields changed; alarm. */
  case ExpiryFenceChanged
  /** The fence was not recorded, or the result makes no sense: no outcome; alarm. */
  case ExpiryUnresolved

  // Fenced cancels: the fence table of README section 8, and its replay.
  /** The authorization came first; the void released the hold. */
  case CancelReleasedHold
  /** The authorization came first, but its hold had already timed out. */
  case CancelFoundHoldExpired
  /** The authorization failed with a transient error; nothing was ever held. */
  case CancelNothingHeld
  /** The cancel came first: the fence took `A`, and a late authorization is locked out. */
  case CancelFencedFirst
  /** The authorization, then its capture: `cancel_refused`. */
  case CancelFoundCapture
  /** An earlier fence holds `A`. */
  case CancelFoundFence
  /** Replay: the void returns `exists`. */
  case CancelReplayedVoid
  /** Replay: the void returns `id_already_failed`, because `pending_transfer_not_found` burned `V`. */
  case CancelReplayedNothingHeld
  /** The fence failed with a transient code, which still burns `A`; alarm. */
  case CancelFenceBurned
  /** The fence's fields changed; alarm. */
  case CancelFenceChanged
  /** A combination the table does not list: no outcome; alarm. */
  case CancelUnresolved

  // Captures: the capture table of README section 8.
  /** A live hold: captured. */
  case CaptureCreated
  /** Its own earlier capture, resent or replayed: captured, with the same amount. */
  case CaptureReplayed
  /** Its own earlier capture with a different amount. */
  case CaptureKeyReused
  /** A hold voided by a cancel. */
  case CaptureFoundVoid
  /** A hold past its timeout. */
  case CaptureFoundExpired
  /** A hold smaller than the amount; a corrected capture may reuse `P`. */
  case CaptureExceedsHold
  /** A fence instead of a hold. */
  case CaptureFoundFence
  /** No hold at all; this burns `P`. */
  case CaptureFoundNoHold
  /** A resend after no hold at all: `id_already_failed`. */
  case CaptureReplayedNoHold
  /** Another rejection by TigerBeetle. */
  case CaptureRejected
  /** A result no row lists: no outcome; alarm. */
  case CaptureUnresolved

  // The edge's request flow (README sections 4, 5 and 8).
  /** An authorization whose key's period has not started: refused without writing. */
  case EdgeRefusedPeriodNotStarted
  /** An authorization whose period has ended: the edge fences `A` instead. */
  case EdgePeriodEndedFencePath
  /** The command log did not acknowledge the command: refused, and the ledger never called. */
  case EdgeRefusedCommandNotAcknowledged
  /** The command was acknowledged too late to leave time for the ledger call and the outcome write. */
  case EdgeRefusedAcknowledgedTooLate
  /** A decision answered after its outcome was acknowledged. */
  case EdgeAnswered
  /** An authorization without a definitive answer: timed out, and the edge starts its own cancel. */
  case EdgeTimedOutAuthorization
  /** A cancel or a capture without an answer: retry, and never a cancel. */
  case EdgeTimedOutRetry
  /** The outcome was not acknowledged: answered as on a timeout. */
  case EdgeOutcomeNotAcknowledged
  /** The ledger call failed without results: answered as on a timeout. */
  case EdgeLedgerCallFailed
  /** A ledger result that came after the deadline, logged anyway. */
  case EdgeLateDecisionLogged
  /** No ledger result by the end of the late wait; the resolver will give the outcome. */
  case EdgeLateResultAbandoned

  // The edge process: its requests under admission control (README section 11, processes).
  /** The edge's own cancel found its partition without a leader: dropped, since the caller's cancel is the guarantee. */
  case EdgeOwnCancelDropped
  /** A reply for a request that has ended, such as the acknowledgement of a late decision's outcome. */
  case EdgeReplyAfterEnd

  // Admission control (README section 12, gap 1).
  case AdmissionRefusedOverload
  case AdmissionRefusedNoLeader
  case AdmissionRetryNoLeader
  case AdmissionRetryOverload
  case AdmissionCancelDuringOverload

  // The resolver (README section 8, resolver).
  /** A straggling authorization gets the fence, never a submission. */
  case ResolverFencedAuthorization
  case ResolverResubmittedCancel
  case ResolverResubmittedCapture
  /** Judged at the end of an idle partition, by the resolver's clock. */
  case ResolverJudgedIdlePartition
  /** An outcome read for a command still waiting. */
  case ResolverFoundOutcome
  /** An outcome read while the resolver's own call was in flight: its result will be ignored. */
  case ResolverOutcomeWhileInFlight
  /** A result for work the resolver no longer has, such as before a rebalance. */
  case ResolverStaleResult
  case ResolverRetryScheduled
  case ResolverRevoked
  /** A record read again, below the next offset. */
  case ResolverRedelivered

  // The caller's contracts (README section 8, who cancels).
  case CallerCancelsAfterNonDefinitive
  case CallerResendsRefused
  case CallerResendsCancel
  case CallerResendsCapture
  /** An event the contract does not allow in this phase, ignored. */
  case CallerIgnoredOutOfTurn
