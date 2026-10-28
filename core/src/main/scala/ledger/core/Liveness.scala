package ledger.core

import scala.concurrent.duration.FiniteDuration

/** The liveness and timeliness properties of README section 11, checked once faults stop. Unlike the safety
  * invariants they need the time of each event and the point where faults stopped.
  */
object Liveness:
  val Answered = "Every request that reaches a live edge instance gets an answer within the edge deadline"
  val CommandHasOutcome = "Every command in the log gets an outcome in the log"
  val CancelAcknowledged = "Every cancel is acknowledged"
  val HoldEnds = "Every hold ends: captured, voided, or expired by TigerBeetle within its timeout plus the expiry lag"
  val ThroughputFloor = "Under overload, useful throughput stays above a floor"

  final case class Bounds(
      /** The edge deadline plus the network between the caller and the edge, on the history clock. */
      answerWithin: FiniteDuration,
      /** The deadline plus the resolver's grace period, plus slack. */
      outcomeWithin: FiniteDuration,
      /** How long after faults stop before requests must be answered in time again. */
      settle: FiniteDuration,
      expiryLag: FiniteDuration,
  )

  def check(h: History, b: Bounds): Vector[Violation] =
    answeredInTime(h, b) ++ everyCommandHasOutcome(h, b) ++ everyCancelAcknowledged(h) ++ everyHoldEnds(h, b)

  /** Requests sent once faults have stopped and settled, early enough that the run could see their answer. */
  def answeredInTime(h: History, b: Bounds): Vector[Violation] =
    val from = h.faultsStoppedAt.fold(Long.MinValue)(_ + b.settle.toMillis)
    val within = b.answerWithin.toMillis
    val received = h.caller.collect { case r: CallerRecord.Received => r }.groupBy(_.request)
    h.caller.collect:
      case CallerRecord.Sent(at, request)
          if at >= from && at + within <= h.endedAt &&
            !received.getOrElse(request, Vector.empty).exists(r => r.at >= at && r.at <= at + within) =>
        Violation(Answered, s"$request sent at $at had no answer within ${b.answerWithin}")

  /** An outcome later on the same partition, for every command logged early enough to have been resolved. */
  def everyCommandHasOutcome(h: History, b: Bounds): Vector[Violation] =
    val outcomes = h.log
      .collect { case LogRecord(p, offset, _, Record.OutcomeRecord(c, _, _)) => ((p, c.id), offset.value) }
      .groupMap(_._1)(_._2)
    h.log.collect:
      case LogRecord(p, offset, at, Record.CommandRecord(c))
          if at + b.outcomeWithin.toMillis <= h.endedAt && !outcomes.getOrElse((p, c.id), Vector.empty).exists(_ > offset.value) =>
        Violation(CommandHasOutcome, s"${c.id} at partition ${p.index}, offset ${offset.value} has no outcome")

  def everyCancelAcknowledged(h: History): Vector[Violation] =
    val sent = h.caller.collect { case CallerRecord.Sent(_, r: Request.Cancel) => r.key }.distinct
    val answered = h.caller.collect { case CallerRecord.Received(_, r: Request.Cancel, Reply.Decided(_)) => r.key }.toSet
    sent.filterNot(answered).map(key => Violation(CancelAcknowledged, s"the cancel for $key was never acknowledged"))

  /** A scan cannot see expiries, so expiry is checked only when the run observed it, as the simulator does. */
  def everyHoldEnds(h: History, b: Bounds): Vector[Violation] =
    h.ledger.transfers.filter(_.transfer.isPending).flatMap: hold =>
      val ended = h.ledger.posts(hold.id).nonEmpty || h.ledger.voids(hold.id).nonEmpty
      (ended, hold.expiresAt, h.ledger.expiries) match
        case (true, _, _) => Vector.empty
        case (false, None, _) => Vector(Violation(HoldEnds, s"hold ${hold.id} has no timeout and never ended"))
        case (false, Some(at), Some(seen)) =>
          val due = at + b.expiryLag.toNanos
          seen.get(hold.id) match
            case Some(expiredAt) if expiredAt > due => Vector(Violation(HoldEnds, s"hold ${hold.id} expired at $expiredAt, after $due"))
            case None if h.ledger.scannedAt.get(hold.cluster).exists(_ >= due) =>
              Vector(Violation(HoldEnds, s"hold ${hold.id} was still held at the scan, past $due"))
            case _ => Vector.empty
        case (false, Some(_), None) => Vector.empty

  /** Decisions answered per window across an overload, each at least the floor. */
  def throughputFloor(h: History, from: Long, to: Long, window: FiniteDuration, floor: Int): Vector[Violation] =
    val w = window.toMillis
    val decided = h.caller.collect { case CallerRecord.Received(at, _, Reply.Decided(_)) if at >= from && at < to => at }
    (from to (to - w) by w).toVector.flatMap: start =>
      val n = decided.count(at => at >= start && at < start + w)
      if n < floor then Vector(Violation(ThroughputFloor, s"$n decisions in [$start, ${start + w}), below $floor")) else Vector.empty
