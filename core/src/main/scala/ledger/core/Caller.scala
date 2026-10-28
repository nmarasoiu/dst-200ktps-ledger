package ledger.core

/** The caller's contracts for one client key (README section 8, who cancels; section 5, refusals). The simulated
  * caller and the stage 6 workload driver both follow this machine. What the caller may choose arrives as an
  * event (resend, capture, stop); what it must do, the machine does.
  *
  *   - After a refusal, it may resend the authorization with the same key, or stop.
  *   - After any non-definitive result it must cancel, resending the cancel until it is answered, and never resend
  *     the authorization.
  *   - After an approval it may capture, resending the capture with the same key until it is answered.
  */
object Caller:

  enum Phase derives CanEqual:
    case AwaitAuthorization
    case Refused
    case Approved
    case Cancelling
    case Capturing(amount: CaptureAmount)
    case Done

  final case class State(authorization: Request.Authorize, phase: Phase) derives CanEqual:
    def key: ClientKey = authorization.key

  enum Event derives CanEqual:
    case Replied(reply: Reply)
    /** A connection error or no answer: non-definitive. */
    case Lost
    case Resend
    case Capture(amount: CaptureAmount)
    case Stop

  enum Effect derives CanEqual:
    case Send(request: Request)

  import Phase.*

  def start(authorization: Request.Authorize): Step[State, Effect] =
    Step(State(authorization, AwaitAuthorization), Vector(Effect.Send(authorization)))

  def step(s: State, e: Event): Step[State, Effect] =
    import Event.*
    (s.phase, e) match
      case (AwaitAuthorization, Replied(Reply.Decided(AuthOutcome.Approved))) => Step(s.copy(phase = Approved))
      case (AwaitAuthorization, Replied(Reply.Decided(AuthOutcome.Declined(_)))) => Step(s.copy(phase = Done))
      case (AwaitAuthorization, Replied(Reply.Refused(_))) => Step(s.copy(phase = Refused))
      case (AwaitAuthorization, Replied(_) | Lost) => cancel(s).mark(Mark.CallerCancelsAfterNonDefinitive)

      case (Refused, Resend) => Step(s.copy(phase = AwaitAuthorization), Vector(Effect.Send(s.authorization)), Vector(Mark.CallerResendsRefused))
      case (Refused | Approved, Stop) => Step(s.copy(phase = Done))

      case (Approved, Capture(amount)) =>
        Step(s.copy(phase = Capturing(amount)), Vector(Effect.Send(Request.Capture(s.key, amount))))

      case (Cancelling, Replied(Reply.Decided(_: CancelOutcome))) => Step(s.copy(phase = Done))
      case (Cancelling, Replied(_) | Lost) => cancel(s).mark(Mark.CallerResendsCancel)

      case (Capturing(_), Replied(Reply.Decided(_: CaptureOutcome))) => Step(s.copy(phase = Done))
      case (Capturing(amount), Replied(_) | Lost) =>
        Step(s, Vector(Effect.Send(Request.Capture(s.key, amount))), Vector(Mark.CallerResendsCapture))

      case _ => Step(s, marks = Vector(Mark.CallerIgnoredOutOfTurn))

  private def cancel(s: State): Step[State, Effect] =
    Step(s.copy(phase = Cancelling), Vector(Effect.Send(Request.Cancel(s.key))))
