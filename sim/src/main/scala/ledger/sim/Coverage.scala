package ledger.sim

import ledger.core.*

/** How often each coverage mark was hit. */
final class MarkCounts:
  private val counts = new Array[Long](Mark.values.length)

  def apply(m: Mark): Long = counts(m.ordinal)
  def add(marks: Iterable[Mark]): Unit = marks.foreach(m => counts(m.ordinal) += 1)
  def addAll(that: MarkCounts): Unit = counts.indices.foreach(i => counts(i) += that.counts(i))
  def hit: Set[Mark] = Mark.values.filter(apply(_) > 0).toSet

  /** One line per mark, in the order `Mark` lists them. */
  def report: String = Mark.values.map(m => f"${apply(m)}%12d  $m").mkString("\n")

/** What it takes for a run to hit each mark (README section 11, coverage marks). The assignment comes from reading
  * the rules (*estimate*), for the simulator to confirm once its adapters exist.
  */
object Coverage:

  enum Scenario derives CanEqual:
    /** Faults alone, with a correct pipeline and a caller that keeps its contracts. The workload includes declines
      * for lack of funds, partial captures, captures above the hold, abandoned holds and holds with short timeouts.
      */
    case Faults
    /** A caller that breaks its contracts in ways the ledger settles safely: it sends a key twice, reuses a key with
      * a different payload, sends a malformed request, captures without an approval or after a cancel, or cancels
      * after a capture. A caller that breaks them unsafely is a planted bug instead.
      */
    case ContractBreakingCaller
    /** Fence accounts closed or missing, fence fields changed between instances, or an account closed under a hold. */
    case MisconfiguredLedger
    /** A period schedule with a switch inside the run (README section 4, one cluster per period). */
    case PeriodSwitch

  import Mark.*
  import Scenario.*

  /** The scenario that reaches a mark, or none when no correct ledger gives its result, so only an example test hits
    * it. No wildcard: a new mark fails the build here until it is placed.
    */
  def scenarioOf(m: Mark): Option[Scenario] = m match
    case AuthorizationCreated | AuthorizationFenced | AuthorizationDeclined | ExpiryFoundAuthorization |
        ExpiryFoundDeclined | ExpiryFenced | ExpiryFoundFence | CancelReleasedHold | CancelFoundHoldExpired |
        CancelNothingHeld | CancelFencedFirst | CancelFoundFence | CancelReplayedVoid | CancelReplayedNothingHeld |
        CaptureCreated | CaptureReplayed | CaptureFoundExpired | CaptureExceedsHold | EdgeRefusedCommandNotAcknowledged |
        EdgeRefusedAcknowledgedTooLate | EdgeAnswered | EdgeTimedOutAuthorization | EdgeTimedOutRetry |
        EdgeOutcomeNotAcknowledged | EdgeLedgerCallFailed | EdgeLateDecisionLogged | EdgeLateResultAbandoned |
        EdgeOwnCancelDropped | EdgeReplyAfterEnd | AdmissionRefusedOverload | AdmissionRefusedNoLeader |
        AdmissionRetryNoLeader | AdmissionRetryOverload | AdmissionCancelDuringOverload | ResolverFencedAuthorization |
        ResolverResubmittedCancel | ResolverResubmittedCapture | ResolverJudgedIdlePartition | ResolverFoundOutcome |
        ResolverOutcomeWhileInFlight | ResolverStaleResult | ResolverRetryScheduled | ResolverRevoked |
        ResolverRedelivered | CallerCancelsAfterNonDefinitive | CallerResendsRefused | CallerResendsCancel |
        CallerResendsCapture | CallerAbandonedHold | CallerIgnoredOutOfTurn =>
      Some(Faults)
    // A caller that keeps its contracts gets at most one copy of an authorization to TigerBeetle, and captures only
    // after an approval, cancels only after a non-definitive result.
    case AuthorizationExists | AuthorizationIdAlreadyFailed | AuthorizationKeyReused | AuthorizationRejected |
        CancelFoundCapture | CaptureKeyReused | CaptureFoundVoid | CaptureFoundFence | CaptureFoundNoHold |
        CaptureReplayedNoHold =>
      Some(ContractBreakingCaller)
    case ExpiryFenceBurned | ExpiryFenceChanged | ExpiryUnresolved | CancelFenceBurned | CancelFenceChanged |
        CancelUnresolved | CaptureRejected =>
      Some(MisconfiguredLedger)
    case EdgeRefusedPeriodNotStarted | EdgePeriodEndedFencePath => Some(PeriodSwitch)
    case AuthorizationUnresolved | CaptureUnresolved => None

  /** The marks a run of seeds in this scenario must hit. */
  def required(s: Scenario): Set[Mark] = Mark.values.filter(scenarioOf(_).contains(s)).toSet
