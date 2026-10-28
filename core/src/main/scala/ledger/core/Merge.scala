package ledger.core

/** The one merge of outcomes every reader uses: the offline checker, the audit log and every other consumer
  * (README section 5, one merge for every reader).
  *
  * A hold's merged value is the set of every outcome seen for it, by command, so merging is set union: the result
  * does not depend on the order outcomes arrive in, and a duplicate changes nothing. The decisions are read from
  * those sets. Two different decisions for one command, or two different endings for one hold, are reported as a
  * conflict rather than merged: they break a safety invariant.
  */
object Merge:

  enum Conflict derives CanEqual:
    case TwoDecisions(id: CommandId, outcomes: Set[Outcome])
    case TwoEndings(a: TransferId, outcomes: Set[Outcome])

  /** How a hold ended. */
  enum Ending derives CanEqual:
    case Open
    case Cancelled
    /** The amount is unknown when the capture is known only from a cancel that found it. */
    case Captured(amount: Option[CaptureAmount])

  final case class Hold(
      a: TransferId,
      authorization: Set[AuthOutcome],
      cancel: Set[CancelOutcome],
      capture: Set[CaptureOutcome],
  ) derives CanEqual:
    def merge(that: Hold): Hold =
      Hold(a, authorization ++ that.authorization, cancel ++ that.cancel, capture ++ that.capture)

    def outcomes: Set[Outcome] = authorization.toSet[Outcome] ++ cancel ++ capture

    /** Approved, or declined with the most specific reason. A copy declined because it reused the key with a
      * different payload can sit beside the approval of the copy that holds the key: its alarm is the signal
      * (README section 12, follow-up 6).
      */
    def authorizationDecision: Either[Conflict, Option[AuthOutcome]] =
      val declined = authorization.collect { case d: AuthOutcome.Declined => d }
      val besideApproval = declined.forall(_.reason.isInstanceOf[DeclineReason.KeyReused])
      if authorization.isEmpty then Right(None)
      else if !authorization.contains(AuthOutcome.Approved) then Right(Some(declined.maxBy(_.reason)))
      else if besideApproval then Right(Some(AuthOutcome.Approved))
      else Left(Conflict.TwoDecisions(CommandId(a, Kind.Authorization), authorization.toSet))

    def cancelDecision: Either[Conflict, Option[CancelOutcome]] =
      if cancel.size > 1 then Left(Conflict.TwoDecisions(CommandId(a, Kind.Cancel), cancel.toSet)) else Right(cancel.headOption)

    /** Captured, or refused with the most specific reason. A refusal TigerBeetle did not record, or one about a
      * different amount, can sit beside a capture without contradicting it (README section 8, captures).
      */
    def captureDecision: Either[Conflict, Option[CaptureOutcome]] =
      val captured = capture.collect { case c: CaptureOutcome.Captured => c }
      val refused = capture.collect { case r: CaptureOutcome.CaptureRefused => r }
      val besideCapture = refused.forall:
        _.reason match
          case CaptureRefusal.ExceedsHold | CaptureRefusal.KeyReused(_) => true
          case _ => false
      if capture.isEmpty then Right(None)
      else if captured.isEmpty then Right(Some(refused.maxBy(_.reason)))
      else if captured.size == 1 && besideCapture then Right(captured.headOption)
      else Left(Conflict.TwoDecisions(CommandId(a, Kind.Capture), capture.toSet))

    /** A cancel reported `cancelled` decides the ending, whatever approval came before or after it (README section
      * 12, follow-up 7). A cancelled hold that was also captured, or a declined one that was captured, has two.
      */
    def ending: Either[Conflict, Ending] =
      for
        auth <- authorizationDecision
        cancelled <- cancelDecision
        captured <- captureDecision
        ending <- endingOf(auth, cancelled, captured)
      yield ending

    /** Every conflict: those of each command, or else that of the ending. */
    def conflicts: Vector[Conflict] =
      val ofCommands = Vector(authorizationDecision, cancelDecision, captureDecision).collect { case Left(c) => c }
      if ofCommands.nonEmpty then ofCommands else ending.left.toOption.toVector

    private def endingOf(
        auth: Option[AuthOutcome],
        cancelled: Option[CancelOutcome],
        captured: Option[CaptureOutcome],
    ): Either[Conflict, Ending] =
      val capturedAmount = captured.collect { case CaptureOutcome.Captured(amount) => amount }
      val isCancelled = cancelled.contains(CancelOutcome.Cancelled)
      val isCaptured = capturedAmount.nonEmpty || cancelled.contains(CancelOutcome.CancelRefused)
      val isDeclined = auth.exists(_.decision == Decision.Declined)
      if (isCancelled || isDeclined) && isCaptured then Left(Conflict.TwoEndings(a, outcomes))
      else if isCancelled then Right(Ending.Cancelled)
      else if isCaptured then Right(Ending.Captured(capturedAmount))
      else Right(Ending.Open)

  object Hold:
    def empty(a: TransferId): Hold = Hold(a, Set.empty, Set.empty, Set.empty)

    def of(a: TransferId, outcome: Outcome): Hold = outcome match
      case o: AuthOutcome => empty(a).copy(authorization = Set(o))
      case o: CancelOutcome => empty(a).copy(cancel = Set(o))
      case o: CaptureOutcome => empty(a).copy(capture = Set(o))

  /** Every outcome, keyed by the hold `A` it concerns. */
  def fold(outcomes: Iterable[(TransferId, Outcome)]): Map[TransferId, Hold] =
    outcomes.foldLeft(Map.empty[TransferId, Hold]):
      case (acc, (a, outcome)) =>
        val one = Hold.of(a, outcome)
        acc.updated(a, acc.get(a).fold(one)(_.merge(one)))
