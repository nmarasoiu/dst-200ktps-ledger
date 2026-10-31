package ledger.core

import AuthOutcome.{Approved, Declined}
import CancelOutcome.{CancelRefused, Cancelled}
import CaptureOutcome.{CaptureRefused, Captured}
import DeclineReason.{NotExecuted, Unknown}
import TbStatus.*

/** One example per row of the tables in README sections 5 and 8, each asserting its coverage mark. */
class RulesSuite extends munit.FunSuite:

  private def assertDecided[O](v: Verdict[O], outcome: O, mark: Mark, alarm: Boolean = false)(using munit.Location): Unit =
    v match
      case Verdict.Decided(o, m, a) =>
        assertEquals(o, outcome)
        assertEquals(m, mark)
        assertEquals(a.isDefined, alarm, s"alarm: $a")
      case other => fail(s"expected $outcome, got $other")

  private def assertUnresolved[O](v: Verdict[O], mark: Mark)(using munit.Location): Unit =
    v match
      case Verdict.Unresolved(m, _) => assertEquals(m, mark)
      case other => fail(s"expected no outcome, got $other")

  // README section 5: the authorization's own result.

  test("section 5: the edge's authorization is created: approved"):
    assertDecided(Rules.authorization(Created), Approved, Mark.AuthorizationCreated)

  test("section 5: the same client key twice: the original approval comes back as exists"):
    assertDecided(Rules.authorization(Exists), Approved, Mark.AuthorizationExists)

  test("section 5: a paused edge's authorization after the resolver's fence: not_executed"):
    assertDecided(Rules.authorization(ExistsWithDifferentFlags), Declined(NotExecuted), Mark.AuthorizationFenced)

  test("section 5: a cancel overtakes its authorization: the late authorization is not_executed"):
    assertDecided(Rules.authorization(ExistsWithDifferentFlags), Declined(NotExecuted), Mark.AuthorizationFenced)

  test("section 5: a refused authorization resent after an earlier copy failed: declined, reason unknown"):
    assertDecided(Rules.authorization(IdAlreadyFailed), Declined(Unknown), Mark.AuthorizationIdAlreadyFailed)

  test("section 5: a transient failure is declined with TigerBeetle's reason"):
    assertDecided(Rules.authorization(ExceedsCredits), Declined(DeclineReason.Ledger(ExceedsCredits)), Mark.AuthorizationDeclined)

  test("section 12, follow-up 6: a key reused with a different amount is told apart from a fence by its code"):
    assertDecided(
      Rules.authorization(ExistsWithDifferentAmount),
      Declined(DeclineReason.KeyReused(ExistsWithDifferentAmount)),
      Mark.AuthorizationKeyReused,
      alarm = true,
    )

  test("a validation failure is declined and not recorded"):
    assertDecided(Rules.authorization(AccountsMustBeDifferent), Declined(DeclineReason.Ledger(AccountsMustBeDifferent)), Mark.AuthorizationRejected)

  // README section 5 and section 8, resolver: the fence written for a straggler.

  test("section 5: the edge crashed after calling the ledger: the fence finds the authorization, approved"):
    assertDecided(Rules.expiry(ExistsWithDifferentFlags), Approved, Mark.ExpiryFoundAuthorization)

  test("section 5: the edge crashed after a declined call: the fence finds the burned ID, declined"):
    assertDecided(Rules.expiry(IdAlreadyFailed), Declined(Unknown), Mark.ExpiryFoundDeclined)

  test("section 5: the edge crashed before calling the ledger: the fence takes A, not_executed"):
    assertDecided(Rules.expiry(Created), Declined(NotExecuted), Mark.ExpiryFenced)

  test("section 5: a zombie resolver's fence meets the first fence: not_executed again"):
    assertDecided(Rules.expiry(Exists), Declined(NotExecuted), Mark.ExpiryFoundFence)

  test("a fence that fails transiently still burns A: not_executed, with an alarm"):
    assertDecided(Rules.expiry(DebitAccountNotFound), Declined(NotExecuted), Mark.ExpiryFenceBurned, alarm = true)

  test("a fence that meets a fence with different fields: not_executed, with an alarm"):
    assertDecided(Rules.expiry(ExistsWithDifferentCode), Declined(NotExecuted), Mark.ExpiryFenceChanged, alarm = true)

  test("a fence rejected by validation was not recorded, so A is not fenced: no outcome"):
    assertUnresolved(Rules.expiry(LedgerMustNotBeZero), Mark.ExpiryUnresolved)

  // README section 8: the fence table, and its replay.

  test("fence table: the authorization first; the void releases the hold"):
    assertDecided(Rules.cancel(ExistsWithDifferentFlags, Created), Cancelled, Mark.CancelReleasedHold)

  test("fence table: the authorization first, already timed out"):
    assertDecided(Rules.cancel(ExistsWithDifferentFlags, PendingTransferExpired), Cancelled, Mark.CancelFoundHoldExpired)

  test("fence table: the authorization failed transiently; nothing was held"):
    assertDecided(Rules.cancel(IdAlreadyFailed, PendingTransferNotFound), Cancelled, Mark.CancelNothingHeld)

  test("fence table: the cancel first; the fence takes A and the void fails harmlessly"):
    assertDecided(Rules.cancel(Created, PendingTransferNotPending), Cancelled, Mark.CancelFencedFirst)

  test("fence table: the authorization, then its capture: cancel_refused"):
    assertDecided(Rules.cancel(ExistsWithDifferentFlags, PendingTransferAlreadyPosted), CancelRefused, Mark.CancelFoundCapture)

  test("a cancel's fence meets the resolver's fence: cancelled"):
    assertDecided(Rules.cancel(Exists, PendingTransferNotPending), Cancelled, Mark.CancelFoundFence)

  test("replay: a released hold's void returns exists, and the outcome repeats"):
    assertDecided(Rules.cancel(ExistsWithDifferentFlags, Exists), Cancelled, Mark.CancelReplayedVoid)

  test("replay: a void that burned V returns id_already_failed, and the outcome repeats"):
    assertDecided(Rules.cancel(IdAlreadyFailed, IdAlreadyFailed), Cancelled, Mark.CancelReplayedNothingHeld)

  test("replay: a fence created by the first try returns exists"):
    assertDecided(Rules.cancel(Exists, PendingTransferNotPending), Cancelled, Mark.CancelFoundFence)

  test("a fence that fails transiently burns A: cancelled, with an alarm"):
    assertDecided(Rules.cancel(CreditAccountAlreadyClosed, PendingTransferNotFound), Cancelled, Mark.CancelFenceBurned, alarm = true)
    assertDecided(Rules.cancel(CreditAccountAlreadyClosed, IdAlreadyFailed), Cancelled, Mark.CancelFenceBurned, alarm = true)

  test("a fence with changed fields still holds A: cancelled, with an alarm"):
    assertDecided(Rules.cancel(ExistsWithDifferentLedger, PendingTransferNotPending), Cancelled, Mark.CancelFenceChanged, alarm = true)

  test("a fence rejected by validation leaves A unfenced: never reported cancelled"):
    assertUnresolved(Rules.cancel(LedgerMustNotBeZero, PendingTransferNotFound), Mark.CancelUnresolved)

  test("a combination no row lists is never reported cancelled"):
    assertUnresolved(Rules.cancel(Created, Created), Mark.CancelUnresolved)
    assertUnresolved(Rules.cancel(ExistsWithDifferentFlags, PendingTransferAlreadyVoided), Mark.CancelUnresolved)

  // README section 8: the capture table.

  private val half = CaptureAmount.Partial(50)

  test("capture table: a live hold: captured, with the amount"):
    assertDecided(Rules.capture(Created, half), Captured(half), Mark.CaptureCreated)

  test("capture table: its own earlier capture, resent: captured with the same amount"):
    assertDecided(Rules.capture(Exists, CaptureAmount.Full), Captured(CaptureAmount.Full), Mark.CaptureReplayed)

  test("capture table: its own earlier capture with a different amount"):
    assertDecided(
      Rules.capture(ExistsWithDifferentAmount, half),
      CaptureRefused(CaptureRefusal.KeyReused(ExistsWithDifferentAmount)),
      Mark.CaptureKeyReused,
      alarm = true,
    )

  test("capture table: a hold voided by a cancel"):
    assertDecided(Rules.capture(PendingTransferAlreadyVoided, half), CaptureRefused(CaptureRefusal.Cancelled), Mark.CaptureFoundVoid)

  test("capture table: a hold past its timeout"):
    assertDecided(Rules.capture(PendingTransferExpired, half), CaptureRefused(CaptureRefusal.Expired), Mark.CaptureFoundExpired)

  test("capture table: a hold smaller than the amount"):
    assertDecided(Rules.capture(ExceedsPendingTransferAmount, half), CaptureRefused(CaptureRefusal.ExceedsHold), Mark.CaptureExceedsHold)

  test("capture table: a fence"):
    assertDecided(Rules.capture(PendingTransferNotPending, half), CaptureRefused(CaptureRefusal.NotApproved), Mark.CaptureFoundFence)

  test("capture table: no hold at all, then its resend"):
    assertDecided(Rules.capture(PendingTransferNotFound, half), CaptureRefused(CaptureRefusal.NotApproved), Mark.CaptureFoundNoHold)
    assertDecided(Rules.capture(IdAlreadyFailed, half), CaptureRefused(CaptureRefusal.NotApproved), Mark.CaptureReplayedNoHold)

  test("a post that cannot happen gives no outcome"):
    assertUnresolved(Rules.capture(PendingTransferAlreadyPosted, half), Mark.CaptureUnresolved)

  // The transfers each command becomes.

  test("an authorization is a pending transfer with ID A and the stamped hold timeout"):
    val c = Fixtures.authCommand(Fixtures.key(1)).asInstanceOf[Command.Authorize]
    val t = Transfers.authorization(c, Fixtures.LedgerCfg)
    assertEquals((t.id, t.flags, t.timeoutSeconds), (c.a, Set(TransferFlag.Pending), Fixtures.HoldTimeout))

  test("a cancel is the fence, then the void, with amount 0 and fields inherited"):
    val a = Ids.authorization(Fixtures.key(1))
    val Vector(fence, void) = Transfers.cancel(a, Fixtures.LedgerCfg): @unchecked
    assertEquals((fence.id, fence.amount, fence.flags, fence.pendingId), (a, U128.Zero, Set.empty[TransferFlag], TransferId.Zero))
    assertEquals((void.id, void.pendingId, void.amount, void.ledger, void.code), (Ids.void(a), a, U128.Zero, 0, 0))

  test("every fence is identical, whoever writes it"):
    val c = Fixtures.authCommand(Fixtures.key(1))
    assertEquals(Transfers.forResolver(c, Fixtures.LedgerCfg), Transfers.forEdge(c, fencePath = true, Fixtures.LedgerCfg))
    assertEquals(Transfers.forResolver(c, Fixtures.LedgerCfg).head, Transfers.cancel(c.a, Fixtures.LedgerCfg).head)

  test("a full capture is sent as the maximum amount, a partial one as its amount; never a raw 0"):
    val a = Ids.authorization(Fixtures.key(1))
    assertEquals(Transfers.post(a, CaptureAmount.Full).amount, U128.Max)
    assertEquals(Transfers.post(a, half).amount, U128.of(50))
    assert(CaptureAmount.Partial.from(0).isLeft)

  test("every mark of the rules is reachable from some result"):
    val statuses = TbStatus.values.toVector
    val reached =
      statuses.flatMap(s => Vector(Rules.authorization(s), Rules.expiry(s), Rules.capture(s, half))) ++
        (for f <- statuses; v <- statuses yield Rules.cancel(f, v))
    val marks = reached.map:
      case Verdict.Decided(_, m, _) => m
      case Verdict.Unresolved(m, _) => m
    val ruleMarks = Mark.values.toSet.filter(m =>
      Vector("Authorization", "Expiry", "Cancel", "Capture").exists(m.toString.startsWith)
    ) - Mark.AuthorizationUnresolved
    assertEquals(ruleMarks -- marks.toSet, Set.empty[Mark])

  test("the numeric codes match TigerBeetle's source for the codes the rules name"):
    assertEquals(Created.code, 0xffffffffL)
    assertEquals(ExistsWithDifferentFlags.code, 36L)
    assertEquals(Exists.code, 46L)
    assertEquals(IdAlreadyFailed.code, 68L)
    assertEquals(PendingTransferNotFound.code, 25L)
    assertEquals(PendingTransferNotPending.code, 26L)
    assertEquals(PendingTransferExpired.code, 35L)
    assertEquals(TbStatus.values.map(_.code).distinct.length, TbStatus.values.length)

