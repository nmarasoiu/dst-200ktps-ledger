package ledger.core

/** The safety invariants of README section 11, as predicates over a history. They hold at every point of every
  * run, whatever the faults and the clock skew; each returns the violations it finds.
  */
object Invariants:
  val NoTransferTwice = "No transfer is applied twice"
  val OneOutcome = "No request reaches two different terminal outcomes"
  val CancelLeavesNoHold = "Once a cancel is reported cancelled, no hold survives and no capture posts it"
  val CaptureOnce = "A hold is captured at most once, for no more than it held"
  val LoggedBeforeAnswered = "Whenever the edge answered with a decision, that decision was already acknowledged in the command log"
  val MatchesLedger = "Every decision matches the ledger"
  val RefusalLeavesNoHold = "If a caller stopped resending an authorization after a refusal, no hold was ever created for its key"
  val BalancesWithinLimits = "Balances never break their configured limits"
  val ApprovalStands = "With a caller that keeps its contracts, a hold it was told is approved is voided only after it sent a cancel for it"
  val WritesFollowCommands = "Every hold, void and post in the ledger follows a command in the log"

  def check(h: History): Vector[Violation] =
    noTransferAppliedTwice(h) ++ noTwoOutcomes(h) ++ cancelLeavesNoHold(h) ++ capturedAtMostOnce(h) ++
      answeredAfterLogged(h) ++ decisionsMatchLedger(h) ++ refusalLeavesNoHold(h) ++ balancesWithinLimits(h) ++
      writesFollowCommands(h)

  /** `check`, and what only a caller that keeps its contracts is promised. */
  def checkKeptContracts(h: History): Vector[Violation] = check(h) ++ approvalStands(h)

  /** Every outcome seen, by hold: logged by an edge or the resolver, or heard by a caller. */
  def outcomes(h: History): Vector[(TransferId, Outcome)] =
    h.log.collect { case LogRecord(_, _, _, Record.OutcomeRecord(c, o, _)) => (c.a, o) } ++
      h.caller.collect { case CallerRecord.Received(_, r, Reply.Decided(o)) => (r.a, o) }

  def holds(h: History): Map[TransferId, Merge.Hold] = Merge.fold(outcomes(h))

  def noTransferAppliedTwice(h: History): Vector[Violation] =
    h.ledger.duplicateIds.toVector.map((id, ts) => Violation(NoTransferTwice, s"$id appears ${ts.size} times")) ++
      h.ledger.closedMoreThanOnce.toVector.map((a, ts) => Violation(NoTransferTwice, s"hold $a was posted or voided ${ts.size} times"))

  /** Over decisions, with every outcome merged by the one merge, so repeats with a generic reason pass. */
  def noTwoOutcomes(h: History): Vector[Violation] =
    holds(h).values.toVector.flatMap(_.conflicts).map(c => Violation(OneOutcome, c.toString))

  /** A hold reported cancelled was never created, or was voided, or had expired; and it was never posted. A later
    * approval for the same ID does not undo the cancel: the merge keeps it.
    */
  def cancelLeavesNoHold(h: History): Vector[Violation] =
    holds(h).toVector.filter(_._2.cancel.contains(CancelOutcome.Cancelled)).flatMap: (a, _) =>
      h.ledger.pending(a) match
        case None => Vector.empty
        case Some(_) if h.ledger.posts(a).nonEmpty =>
          Vector(Violation(CancelLeavesNoHold, s"hold $a was reported cancelled and was posted"))
        case Some(hold) if h.ledger.voids(a).isEmpty && !h.ledger.expired(hold) =>
          Vector(Violation(CancelLeavesNoHold, s"hold $a was reported cancelled and survives"))
        case Some(_) => Vector.empty

  def capturedAtMostOnce(h: History): Vector[Violation] =
    h.ledger.transfers.filter(_.transfer.isPending).flatMap: hold =>
      val posts = h.ledger.posts(hold.id)
      val twice = if posts.size > 1 then Vector(Violation(CaptureOnce, s"hold ${hold.id} was posted ${posts.size} times")) else Vector.empty
      val over = posts
        .filter(p => Ordering[U128].gt(p.transfer.amount, hold.transfer.amount))
        .map(p => Violation(CaptureOnce, s"hold ${hold.id} of ${hold.transfer.amount.toBigInt} was posted ${p.transfer.amount.toBigInt}"))
      twice ++ over

  def answeredAfterLogged(h: History): Vector[Violation] =
    val logged = h.log
      .collect { case LogRecord(_, _, at, Record.OutcomeRecord(c, o, _)) => (c.id, (o.decision, at)) }
      .groupMap(_._1)(_._2)
    h.caller.collect:
      case CallerRecord.Received(at, request, Reply.Decided(o))
          if !logged.getOrElse(request.id, Vector.empty).exists((d, loggedAt) => d == o.decision && loggedAt <= at) =>
        Violation(LoggedBeforeAnswered, s"${request.id} was answered ${o.decision} at $at, before that decision was logged")

  /** Each command's merged decision against what TigerBeetle holds. Conflicted holds are left to `noTwoOutcomes`. */
  def decisionsMatchLedger(h: History): Vector[Violation] =
    holds(h).toVector.flatMap: (a, hold) =>
      def broken(what: String) = Vector(Violation(MatchesLedger, s"hold $a: $what"))
      val held = h.ledger.pending(a).nonEmpty
      val posts = h.ledger.posts(a)
      val authorization = hold.authorizationDecision.toOption.flatten.toVector.flatMap:
        case AuthOutcome.Approved if !held => broken("approved, but the ledger has no hold")
        // A key reused with a different payload is declined while the copy that holds the key keeps its hold.
        case AuthOutcome.Declined(reason) if held && !reason.isInstanceOf[DeclineReason.KeyReused] =>
          broken("declined, but the ledger holds it")
        case _ => Vector.empty
      val cancel = hold.cancelDecision.toOption.flatten.toVector.flatMap:
        case CancelOutcome.CancelRefused if posts.isEmpty => broken("cancel_refused, but the hold was never posted")
        case _ => Vector.empty
      // Each capture reported, not only the merged one: a full capture must match the post, even beside a partial one.
      val captures = hold.capture.toVector.collect:
        case CaptureOutcome.Captured(amount) if !posts.exists(p => moved(amount, a, h).contains(p.transfer.amount)) =>
          broken(s"captured $amount, but the ledger has no such post")
      val capture = captures.flatten ++ hold.captureDecision.toOption.flatten.toVector.flatMap:
        case CaptureOutcome.CaptureRefused(CaptureRefusal.Cancelled) if h.ledger.voids(a).isEmpty =>
          broken("capture refused as cancelled, but the hold was never voided")
        case CaptureOutcome.CaptureRefused(CaptureRefusal.Expired | CaptureRefusal.NotApproved) if posts.nonEmpty =>
          broken("capture refused, but the hold was posted")
        case _ => Vector.empty
      authorization ++ cancel ++ capture

  /** The amount a capture moves, as the ledger records it: all of the hold when it asked for all of it. */
  private def moved(amount: CaptureAmount, a: TransferId, h: History): Option[U128] = amount match
    case CaptureAmount.Full => h.ledger.pending(a).map(_.transfer.amount)
    case CaptureAmount.Partial(n) => Some(U128.of(n))

  /** A key whose every authorization copy the caller sent came back refused, and was never resent after the last
    * refusal, has no hold. A hidden resend by a hop between the caller and the edge breaks it (README section 5).
    */
  def refusalLeavesNoHold(h: History): Vector[Violation] =
    h.caller.filter(_.request.kind == Kind.Authorization).groupBy(_.request.key).toVector.flatMap: (key, records) =>
      val sent = records.collect { case s: CallerRecord.Sent => s }
      val answers = records.filter:
        case _: CallerRecord.Sent => false
        case _ => true
      val allRefused = answers.forall:
        case CallerRecord.Received(_, _, Reply.Refused(_)) => true
        case _ => false
      val a = Ids.authorization(key)
      if sent.nonEmpty && answers.size == sent.size && allRefused && h.ledger.pending(a).nonEmpty then
        Vector(Violation(RefusalLeavesNoHold, s"every authorization for $key was refused, yet the ledger holds $a"))
      else Vector.empty

  /** A voided authorization still returns `exists`, so a caller that resends after a non-definitive result, instead
    * of cancelling, can hear `approved` about a hold the edge's own cancel released (README section 12, follow-up 7).
    */
  def approvalStands(h: History): Vector[Violation] =
    val cancelling = h.caller.collect { case CallerRecord.Sent(_, r: Request.Cancel) => r.a }.toSet
    val approved = h.caller.collect { case CallerRecord.Received(_, r: Request.Authorize, Reply.Decided(AuthOutcome.Approved)) => r.a }.distinct
    approved.filter(a => !cancelling(a) && h.ledger.voids(a).nonEmpty).map: a =>
      Violation(ApprovalStands, s"hold $a was told approved and voided, and its caller never cancelled")

  /** TigerBeetle's timestamps are on another clock than the log's, so this asks only that the command exists. A
    * transfer with no flags is left out: a fence looks like the setup's plain transfers, and the fence's own rules
    * judge it.
    */
  def writesFollowCommands(h: History): Vector[Violation] =
    val logged = h.log.collect { case LogRecord(_, _, _, Record.CommandRecord(c)) => c.id }.toSet
    h.ledger.transfers.flatMap: t =>
      val needed = t.transfer.flags.headOption.map:
        case TransferFlag.Pending => CommandId(t.id, Kind.Authorization)
        case TransferFlag.VoidPendingTransfer => CommandId(t.transfer.pendingId, Kind.Cancel)
        case TransferFlag.PostPendingTransfer => CommandId(t.transfer.pendingId, Kind.Capture)
      needed.filterNot(logged).map(id => Violation(WritesFollowCommands, s"${t.id} has no $id in the log"))

  /** Replays each cluster in timestamp order, with expiries, and checks every flagged account after each change. */
  def balancesWithinLimits(h: History): Vector[Violation] =
    val limits = h.ledger.accounts.map(acc => (acc.cluster, acc.id) -> acc).toMap
    h.ledger.transfers.groupBy(_.cluster).toVector.flatMap((cluster, transfers) => replay(cluster, transfers, h.ledger, limits))

  private final case class Balance(debitsPending: BigInt, debitsPosted: BigInt, creditsPending: BigInt, creditsPosted: BigInt):
    def +(d: Balance): Balance =
      Balance(debitsPending + d.debitsPending, debitsPosted + d.debitsPosted, creditsPending + d.creditsPending, creditsPosted + d.creditsPosted)

  private val Empty = Balance(0, 0, 0, 0)

  private def replay(
      cluster: Period,
      transfers: Vector[LedgerTransfer],
      ledger: LedgerSnapshot,
      limits: Map[(Period, AccountId), LedgerAccount],
  ): Vector[Violation] =
    val holds = transfers.filter(_.transfer.isPending).map(t => t.id -> t).toMap
    val closed = transfers.filter(t => t.transfer.isPost || t.transfer.isVoid).map(_.transfer.pendingId).toSet
    def release(hold: LedgerTransfer): Vector[(AccountId, Balance)] =
      val amount = hold.transfer.amount.toBigInt
      Vector(hold.transfer.debit -> Empty.copy(debitsPending = -amount), hold.transfer.credit -> Empty.copy(creditsPending = -amount))
    def changes(t: LedgerTransfer): Vector[(AccountId, Balance)] =
      val amount = t.transfer.amount.toBigInt
      if t.transfer.isPending then
        Vector(t.transfer.debit -> Empty.copy(debitsPending = amount), t.transfer.credit -> Empty.copy(creditsPending = amount))
      else if t.transfer.isVoid then holds.get(t.transfer.pendingId).toVector.flatMap(release)
      else if t.transfer.isPost then
        holds.get(t.transfer.pendingId).toVector.flatMap: hold =>
          val posted = t.transfer.amount.toBigInt
          release(hold) ++ Vector(hold.transfer.debit -> Empty.copy(debitsPosted = posted), hold.transfer.credit -> Empty.copy(creditsPosted = posted))
      else Vector(t.transfer.debit -> Empty.copy(debitsPosted = amount), t.transfer.credit -> Empty.copy(creditsPosted = amount))
    val expiries = holds.values.toVector.filterNot(hold => closed(hold.id)).flatMap: hold =>
      val at = ledger.expiries match
        case Some(seen) => seen.get(hold.id)
        case None => hold.expiresAt.filter(at => ledger.scannedAt.get(cluster).exists(at <= _))
      at.map(_ -> release(hold))
    val timeline = (transfers.map(t => t.timestamp -> changes(t)) ++ expiries).sortBy(_._1)
    val (_, violations) = timeline.foldLeft((Map.empty[AccountId, Balance], Vector.empty[Violation])):
      case ((balances, found), (at, deltas)) =>
        val next = deltas.foldLeft(balances)((acc, d) => acc.updated(d._1, acc.getOrElse(d._1, Empty) + d._2))
        val broken = deltas.map(_._1).distinct.flatMap: account =>
          val b = next(account)
          limits.get((cluster, account)).toVector.flatMap: limit =>
            val debits = limit.debitsMustNotExceedCredits && b.debitsPending + b.debitsPosted > b.creditsPosted
            val credits = limit.creditsMustNotExceedDebits && b.creditsPending + b.creditsPosted > b.debitsPosted
            if debits || credits then Vector(Violation(BalancesWithinLimits, s"account $account in period ${cluster.index} at $at: $b")) else Vector.empty
        (next, found ++ broken)
    violations
