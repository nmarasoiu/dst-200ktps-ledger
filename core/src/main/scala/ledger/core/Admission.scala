package ledger.core

import scala.concurrent.duration.FiniteDuration

/** Admission control at the edge (README section 12, gap 1): refuse authorizations fast once the edge is
  * overloaded or the authorization's log partition has no leader. A refusal is cheap, since nothing was sent, and
  * the caller may resend with the same key. Captures are told to retry. Cancels release money, so they are accepted
  * under more load than anything else, but not under any load: a call that misses the deadline is resent while it
  * still queues, and each resend lengthens the queue that made it miss.
  *
  * What counts as overloaded is learned, since what the ledger can serve is not known in advance and changes. The
  * limit on requests in flight falls with each slow reply from the ledger and rises slowly with each fast one, so an
  * instance finds its share of the ledger whatever the other instances do. With nothing in flight one request always
  * goes, and its reply is the news that the ledger is back.
  */
object Admission:

  final case class Config(
      /** The most the limit can reach. */
      maxInFlight: Int,
      /** A reply slower than this means the queue is too long. It sits well inside the deadline. */
      maxLedgerLatency: FiniteDuration,
      partitions: Int,
      /** Where the limit starts: a cold instance has no evidence that the ledger can take more. */
      startInFlight: Int = 16,
      /** How far past the limit a cancel is still accepted, as a multiple of it. */
      cancelHeadroom: Double = 2.0,
  )

  /** `limit` is how many requests this instance lets in flight right now, between 1 and `maxInFlight`. */
  final case class State(inFlight: Int, limit: Double, leaderless: Set[Partition]) derives CanEqual

  object State:
    def initial(cfg: Config): State = State(0, cfg.startInFlight.toDouble.min(cfg.maxInFlight), Set.empty)

  enum Event derives CanEqual:
    case Arrived(request: Request)
    /** An admitted request has ended, and its ledger call, if it made one, has replied. */
    case Finished
    case LedgerLatency(observed: FiniteDuration)
    /** The partitions the producer's metadata shows without a leader (README section 8, failover). */
    case LeadersChanged(leaderless: Set[Partition])

  enum Decision derives CanEqual:
    case Admit
    case Refuse(reason: RefusalReason)
    case Retry

  /** What one slow reply leaves of the limit. A queue of slow replies takes it nearly to one, since a long queue must
    * stop growing now: every call in it that misses the deadline is resent. Fast replies soon bring it back.
    */
  private val Cut = 0.9

  def step(s: State, e: Event, cfg: Config): Step[State, Decision] = e match
    case Event.Arrived(request) =>
      val noLeader = s.leaderless(Partitioner.partition(request.a, cfg.partitions))
      val overloaded = s.inFlight >= s.limit
      val saturated = s.inFlight >= s.limit * cfg.cancelHeadroom
      request match
        case _: Request.Authorize if noLeader => Step(s, Vector(Decision.Refuse(RefusalReason.NoLeader)), Vector(Mark.AdmissionRefusedNoLeader))
        case _: Request.Authorize if overloaded => Step(s, Vector(Decision.Refuse(RefusalReason.Overload)), Vector(Mark.AdmissionRefusedOverload))
        case _ if noLeader => Step(s, Vector(Decision.Retry), Vector(Mark.AdmissionRetryNoLeader))
        case _: Request.Capture if overloaded => Step(s, Vector(Decision.Retry), Vector(Mark.AdmissionRetryOverload))
        case _: Request.Cancel if saturated => Step(s, Vector(Decision.Retry), Vector(Mark.AdmissionRetrySaturated))
        case _: Request.Cancel if overloaded => admit(s).mark(Mark.AdmissionCancelDuringOverload)
        case _ => admit(s)
    case Event.Finished => Step(s.copy(inFlight = (s.inFlight - 1).max(0)))
    case Event.LedgerLatency(observed) =>
      if observed >= cfg.maxLedgerLatency then Step(s.copy(limit = (s.limit * Cut).max(1.0)))
      // A fast reply says nothing about a limit the instance is not using, so it only raises one in use. Far inside
      // the target the queue is nearly empty, so one more for each reply is safe; near it, one more for each limit's
      // worth of replies.
      else if s.inFlight * 2 < s.limit then Step(s)
      else
        val more = if observed < cfg.maxLedgerLatency / 4 then 1.0 else 1 / s.limit
        Step(s.copy(limit = (s.limit + more).min(cfg.maxInFlight)))
    case Event.LeadersChanged(leaderless) => Step(s.copy(leaderless = leaderless))

  private def admit(s: State): Step[State, Decision] = Step(s.copy(inFlight = s.inFlight + 1), Vector(Decision.Admit))
