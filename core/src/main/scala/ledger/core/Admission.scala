package ledger.core

import scala.concurrent.duration.FiniteDuration

/** Admission control at the edge (README section 12, gap 1): refuse authorizations fast once the edge is
  * overloaded or the authorization's log partition has no leader. A refusal is cheap, since nothing was sent, and
  * the caller may resend with the same key. Cancels are accepted whenever their partition has a leader, since
  * they release money; captures are told to retry.
  */
object Admission:

  final case class Config(maxInFlight: Int, maxLedgerLatency: FiniteDuration, partitions: Int)

  /** `ledgerLatencyMillis` is a moving average of the ledger's latency. */
  final case class State(inFlight: Int, ledgerLatencyMillis: Double, leaderless: Set[Partition])

  object State:
    val Initial: State = State(0, 0.0, Set.empty)

  enum Event:
    case Arrived(request: Request)
    /** An admitted request has ended. */
    case Finished
    case LedgerLatency(observed: FiniteDuration)
    /** The partitions the producer's metadata shows without a leader (README section 8, failover). */
    case LeadersChanged(leaderless: Set[Partition])

  enum Decision:
    case Admit
    case Refuse(reason: RefusalReason)
    case Retry

  /** Weight of the newest latency sample in the moving average. */
  private val Smoothing = 0.2

  def step(s: State, e: Event, cfg: Config): Step[State, Decision] = e match
    case Event.Arrived(request) =>
      val noLeader = s.leaderless(Partitioner.partition(request.a, cfg.partitions))
      val overloaded = s.inFlight >= cfg.maxInFlight || s.ledgerLatencyMillis >= cfg.maxLedgerLatency.toMillis
      request match
        case _: Request.Authorize if noLeader => Step(s, Vector(Decision.Refuse(RefusalReason.NoLeader)), Vector(Mark.AdmissionRefusedNoLeader))
        case _: Request.Authorize if overloaded => Step(s, Vector(Decision.Refuse(RefusalReason.Overload)), Vector(Mark.AdmissionRefusedOverload))
        case _ if noLeader => Step(s, Vector(Decision.Retry), Vector(Mark.AdmissionRetryNoLeader))
        case _: Request.Capture if overloaded => Step(s, Vector(Decision.Retry), Vector(Mark.AdmissionRetryOverload))
        case _: Request.Cancel if overloaded => admit(s).mark(Mark.AdmissionCancelDuringOverload)
        case _ => admit(s)
    case Event.Finished => Step(s.copy(inFlight = (s.inFlight - 1).max(0)))
    case Event.LedgerLatency(observed) =>
      Step(s.copy(ledgerLatencyMillis = s.ledgerLatencyMillis * (1 - Smoothing) + observed.toMillis * Smoothing))
    case Event.LeadersChanged(leaderless) => Step(s.copy(leaderless = leaderless))

  private def admit(s: State): Step[State, Decision] = Step(s.copy(inFlight = s.inFlight + 1), Vector(Decision.Admit))
