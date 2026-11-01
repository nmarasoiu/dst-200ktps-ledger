package ledger.contract

import cats.effect.IO
import cats.syntax.all.*
import java.util.concurrent.atomic.AtomicLong
import ledger.core.*
import ledger.core.TbStatus.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** The behavior of TigerBeetle the design depends on (README sections 5, 6 and 8), held against the real system and
  * the simulated one alike. Every transfer the pipeline sends comes from `Transfers`, so this checks what the pipeline
  * really sends. All tests share one cluster, so each makes its own accounts and keys.
  */
abstract class LedgerContract extends CatsEffectSuite:

  def ledger: LedgerUnderTest

  override def munitIOTimeout: Duration = 2.minutes

  private val counter = new AtomicLong(0)
  private val OneHour = 3600
  private val OneSecond = 1
  private val PastOneSecond = 1100.millis
  private val AccountLedger = 7
  private val AccountCode = 70
  private val AuthorizationCode = 71

  private def next(): Long = counter.incrementAndGet()

  private def newId(): AccountId = AccountId(U128(0xc0a7L, next()))

  private val cfg = LedgerConfig(
    fenceDebit = AccountId(U128(0xfe2ceL, 1)),
    fenceCredit = AccountId(U128(0xfe2ceL, 2)),
    fenceLedger = 99,
    fenceCode = 90,
    authorizationCode = AuthorizationCode,
  )

  private final case class Pair(debit: AccountId, credit: AccountId)

  private def account(id: AccountId, ledgerId: Int = AccountLedger) = Ledger.NewAccount(id, ledgerId, AccountCode)

  /** Two fresh accounts, after making sure the fence accounts exist: setting them up again changes nothing. */
  private def accounts(debitLimited: Boolean = false, creditLimited: Boolean = false): IO[Pair] =
    val pair = Pair(newId(), newId())
    ledger
      .createAccounts(
        Vector(
          account(cfg.fenceDebit, cfg.fenceLedger),
          account(cfg.fenceCredit, cfg.fenceLedger),
          account(pair.debit).copy(debitsMustNotExceedCredits = debitLimited),
          account(pair.credit).copy(creditsMustNotExceedDebits = creditLimited),
        ),
      )
      .as(pair)

  private def key(): ClientKey =
    val n = next()
    ClientKey(WallTime(1_700_000_000_000L + n), s"contract-$n")

  private def authorization(key: ClientKey, pair: Pair, amount: Long = 100, timeout: Int = OneHour): Transfer =
    Transfers.authorization(Command.Authorize(key, pair.debit, pair.credit, U128.of(amount), AccountLedger, timeout), cfg)

  private def a(key: ClientKey): TransferId = Ids.authorization(key)

  private def submit(transfers: Transfer*): IO[Vector[TbStatus]] =
    ledger
      .submit(transfers.toVector)
      .map:
        case Ledger.Result.Returned(statuses) => statuses
        case Ledger.Result.Failed => fail("the call failed without statuses")

  private def cancel(key: ClientKey): IO[Vector[TbStatus]] = submit(Transfers.cancel(a(key), cfg)*)

  private def post(key: ClientKey, amount: CaptureAmount): Transfer = Transfers.post(a(key), amount)

  private def balances(id: AccountId): IO[Ledger.Balances] = ledger.lookupAccounts(Vector(id)).map(_.head)

  /** The debit account's balances, as (pending, posted). */
  private def debits(pair: Pair): IO[(Long, Long)] =
    balances(pair.debit).map(b => (b.debitsPending.lo, b.debitsPosted.lo))

  private def fund(pair: Pair, amount: Long): IO[Unit] =
    // A transfer into the limited debit account gives it credits to spend.
    for
      source <- accounts()
      id = TransferId(U128(0xf00dL, next()))
      plain = Transfer(id, source.debit, pair.debit, U128.of(amount), TransferId.Zero, AccountLedger, AuthorizationCode, Set.empty, 0)
      _ <- submit(plain).assertEquals(Vector(Created))
    yield ()

  // The fence table of README section 8, each row replayed once.

  test("the authorization first: the fence finds it, the void releases the hold, and a replay repeats the outcome"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- debits(pair).assertEquals((100L, 0L))
      _ <- cancel(k).assertEquals(Vector(ExistsWithDifferentFlags, Created))
      _ <- debits(pair).assertEquals((0L, 0L))
      _ <- cancel(k).assertEquals(Vector(ExistsWithDifferentFlags, Exists))
      _ <- debits(pair).assertEquals((0L, 0L))
    yield ()

  test("the authorization first, past its timeout: the void finds it expired, and a replay finds the same"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair, timeout = OneSecond)).assertEquals(Vector(Created))
      _ <- ledger.passTime(PastOneSecond)
      _ <- cancel(k).assertEquals(Vector(ExistsWithDifferentFlags, PendingTransferExpired))
      _ <- cancel(k).assertEquals(Vector(ExistsWithDifferentFlags, PendingTransferExpired))
    yield ()

  test("the authorization failed with a transient error: nothing was held, and the void's failure burns V"):
    val k = key()
    for
      pair <- accounts(debitLimited = true)
      _ <- submit(authorization(k, pair)).assertEquals(Vector(ExceedsCredits))
      _ <- cancel(k).assertEquals(Vector(IdAlreadyFailed, PendingTransferNotFound))
      _ <- cancel(k).assertEquals(Vector(IdAlreadyFailed, IdAlreadyFailed))
    yield ()

  test("the cancel first: the fence takes A, the void fails harmlessly, and the late authorization is refused"):
    val k = key()
    for
      pair <- accounts()
      _ <- cancel(k).assertEquals(Vector(Created, PendingTransferNotPending))
      _ <- submit(authorization(k, pair)).assertEquals(Vector(ExistsWithDifferentFlags))
      _ <- debits(pair).assertEquals((0L, 0L))
      _ <- cancel(k).assertEquals(Vector(Exists, PendingTransferNotPending))
      _ <- submit(authorization(k, pair)).assertEquals(Vector(ExistsWithDifferentFlags))
      _ <- debits(pair).assertEquals((0L, 0L))
    yield ()

  test("the authorization, then its capture: the void finds it posted, and nothing is released"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair), post(k, CaptureAmount.Full)).assertEquals(Vector(Created, Created))
      _ <- cancel(k).assertEquals(Vector(ExistsWithDifferentFlags, PendingTransferAlreadyPosted))
      _ <- debits(pair).assertEquals((0L, 100L))
      _ <- cancel(k).assertEquals(Vector(ExistsWithDifferentFlags, PendingTransferAlreadyPosted))
      _ <- debits(pair).assertEquals((0L, 100L))
    yield ()

  // IDs and fences.

  test("a void may reference a hold created earlier in the same batch"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair) +: Transfers.cancel(a(k), cfg).drop(1)*).assertEquals(Vector(Created, Created))
      _ <- debits(pair).assertEquals((0L, 0L))
    yield ()

  test("a voided authorization resubmitted with the same fields returns exists, and holds nothing"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- cancel(k).assertEquals(Vector(ExistsWithDifferentFlags, Created))
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Exists))
      _ <- debits(pair).assertEquals((0L, 0L))
    yield ()

  test("a resubmitted authorization returns exists, and holds once"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Exists))
      _ <- debits(pair).assertEquals((100L, 0L))
    yield ()

  test("a fence that fails with a transient code still burns A"):
    val k = key()
    val missing = cfg.copy(fenceDebit = newId())
    for
      pair <- accounts()
      _ <- submit(Transfers.fence(a(k), missing)).assertEquals(Vector(DebitAccountNotFound))
      _ <- submit(Transfers.fence(a(k), missing)).assertEquals(Vector(IdAlreadyFailed))
      _ <- submit(Transfers.fence(a(k), cfg)).assertEquals(Vector(IdAlreadyFailed))
      _ <- submit(authorization(k, pair)).assertEquals(Vector(IdAlreadyFailed))
    yield ()

  test("a fence that meets an earlier identical fence returns exists, and moves nothing"):
    val k = key()
    for
      _ <- accounts()
      before <- balances(cfg.fenceDebit)
      _ <- submit(Transfers.fence(a(k), cfg)).assertEquals(Vector(Created))
      _ <- submit(Transfers.fence(a(k), cfg)).assertEquals(Vector(Exists))
      _ <- balances(cfg.fenceDebit).assertEquals(before)
    yield ()

  test("flags are compared first, then each other field in TigerBeetle's order"):
    val k = key()
    val first = Transfers.fence(a(k), cfg)
    val other = TransferId(U128(1L, 1L))
    // Differs in every field. Each step puts one field back, so the next difference shows.
    val allDifferent = first.copy(
      debit = newId(),
      credit = newId(),
      amount = U128.of(1),
      pendingId = other,
      ledger = 1,
      code = 1,
      flags = Set(TransferFlag.Pending),
      timeoutSeconds = 1,
    )
    val steps = Vector[(Transfer => Transfer, TbStatus)](
      (identity, ExistsWithDifferentFlags),
      (_.copy(flags = first.flags), ExistsWithDifferentPendingId),
      (_.copy(pendingId = first.pendingId), ExistsWithDifferentTimeout),
      (_.copy(timeoutSeconds = first.timeoutSeconds), ExistsWithDifferentDebitAccountId),
      (_.copy(debit = first.debit), ExistsWithDifferentCreditAccountId),
      (_.copy(credit = first.credit), ExistsWithDifferentAmount),
      (_.copy(amount = first.amount), ExistsWithDifferentLedger),
      (_.copy(ledger = first.ledger), ExistsWithDifferentCode),
      (_.copy(code = first.code), Exists),
    )
    for
      _ <- accounts()
      _ <- submit(first).assertEquals(Vector(Created))
      _ <- steps
        .foldLeft(IO.pure(allDifferent)):
          case (io, (putBack, expected)) =>
            for
              transfer <- io
              changed = putBack(transfer)
              _ <- submit(changed).assertEquals(Vector(expected), clue(changed))
            yield changed
        .void
    yield ()

  test("a hold's timeout and amount are part of a retry's identity"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- submit(authorization(k, pair, timeout = OneHour + 1)).assertEquals(Vector(ExistsWithDifferentTimeout))
      _ <- submit(authorization(k, pair, amount = 101)).assertEquals(Vector(ExistsWithDifferentAmount))
      _ <- debits(pair).assertEquals((100L, 0L))
    yield ()

  test("a batch runs in order: a transient failure burns the ID for the rest of the batch"):
    val k = key()
    for
      pair <- accounts(debitLimited = true)
      _ <- submit(authorization(k, pair), authorization(k, pair)).assertEquals(Vector(ExceedsCredits, IdAlreadyFailed))
    yield ()

  test("a batch's events are not linked: one failing leaves the next alone, and results come in order"):
    val (bad, good) = (key(), key())
    for
      limited <- accounts(debitLimited = true)
      pair <- accounts()
      _ <- submit(authorization(bad, limited), authorization(good, pair)).assertEquals(Vector(ExceedsCredits, Created))
      _ <- debits(pair).assertEquals((100L, 0L))
    yield ()

  test("a batch's event sees what earlier events in it did to a balance"):
    val (one, two) = (key(), key())
    for
      pair <- accounts(debitLimited = true)
      _ <- fund(pair, 150)
      _ <- submit(authorization(one, pair), authorization(two, pair)).assertEquals(Vector(Created, ExceedsCredits))
      _ <- debits(pair).assertEquals((100L, 0L))
    yield ()

  test("balance limits count holds, a void gives them back, and a failed hold burns its ID"):
    val (one, two, three) = (key(), key(), key())
    for
      pair <- accounts(debitLimited = true)
      _ <- fund(pair, 150)
      _ <- submit(authorization(one, pair)).assertEquals(Vector(Created))
      _ <- submit(authorization(two, pair)).assertEquals(Vector(ExceedsCredits))
      _ <- submit(authorization(two, pair)).assertEquals(Vector(IdAlreadyFailed))
      _ <- cancel(one).assertEquals(Vector(ExistsWithDifferentFlags, Created))
      _ <- submit(authorization(three, pair)).assertEquals(Vector(Created))
    yield ()

  test("a credit limit refuses a hold that would pass it"):
    val k = key()
    for
      pair <- accounts(creditLimited = true)
      _ <- submit(authorization(k, pair)).assertEquals(Vector(ExceedsDebits))
      _ <- submit(authorization(k, pair)).assertEquals(Vector(IdAlreadyFailed))
    yield ()

  test("an account's limit does not count zero: a hold of nothing fits any limit"):
    val k = key()
    for
      pair <- accounts(debitLimited = true)
      _ <- submit(authorization(k, pair, amount = 0)).assertEquals(Vector(Created))
    yield ()

  test("a validation failure returns its code and leaves the ID free, so a fence that fails one does not fence A"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(Transfers.fence(a(k), cfg).copy(code = 0)).assertEquals(Vector(CodeMustNotBeZero))
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
    yield ()

  test("every validation failure the pipeline's fields can produce returns its code"):
    val k = key()
    val id = a(k)
    for
      pair <- accounts()
      otherLedger <- newId().pure[IO].flatTap(i => ledger.createAccounts(Vector(account(i, AccountLedger + 1))))
      plain = authorization(k, pair)
      hold = key()
      _ <- submit(authorization(hold, pair)).assertEquals(Vector(Created))
      void = Transfers.void(a(hold))
      capture = Transfers.post(a(hold), CaptureAmount.Partial(1))
      max = TransferId(U128.Max)
      cases = Vector[(Transfer, TbStatus)](
        plain.copy(id = TransferId.Zero) -> IdMustNotBeZero,
        plain.copy(id = max) -> IdMustNotBeIntMax,
        plain.copy(debit = AccountId.Zero) -> DebitAccountIdMustNotBeZero,
        plain.copy(debit = AccountId(U128.Max)) -> DebitAccountIdMustNotBeIntMax,
        plain.copy(credit = AccountId.Zero) -> CreditAccountIdMustNotBeZero,
        plain.copy(credit = AccountId(U128.Max)) -> CreditAccountIdMustNotBeIntMax,
        plain.copy(credit = pair.debit) -> AccountsMustBeDifferent,
        plain.copy(pendingId = id) -> PendingIdMustBeZero,
        plain.copy(flags = Set.empty) -> TimeoutReservedForPendingTransfer,
        plain.copy(ledger = 0) -> LedgerMustNotBeZero,
        plain.copy(code = 0) -> CodeMustNotBeZero,
        plain.copy(credit = otherLedger) -> AccountsMustHaveTheSameLedger,
        plain.copy(ledger = AccountLedger + 1) -> TransferMustHaveTheSameLedgerAsAccounts,
        void.copy(flags = Set(TransferFlag.VoidPendingTransfer, TransferFlag.PostPendingTransfer)) -> FlagsAreMutuallyExclusive,
        void.copy(flags = Set(TransferFlag.VoidPendingTransfer, TransferFlag.Pending)) -> FlagsAreMutuallyExclusive,
        void.copy(pendingId = TransferId.Zero) -> PendingIdMustNotBeZero,
        void.copy(pendingId = max) -> PendingIdMustNotBeIntMax,
        void.copy(pendingId = void.id) -> PendingIdMustBeDifferent,
        void.copy(timeoutSeconds = 1) -> TimeoutReservedForPendingTransfer,
        void.copy(debit = pair.credit) -> PendingTransferHasDifferentDebitAccountId,
        void.copy(credit = pair.debit) -> PendingTransferHasDifferentCreditAccountId,
        void.copy(ledger = AccountLedger + 1) -> PendingTransferHasDifferentLedger,
        void.copy(code = AuthorizationCode + 1) -> PendingTransferHasDifferentCode,
        void.copy(amount = U128.of(1)) -> PendingTransferHasDifferentAmount,
        void.copy(amount = U128.of(101)) -> ExceedsPendingTransferAmount,
        capture.copy(amount = U128.of(101)) -> ExceedsPendingTransferAmount,
      )
      _ <- cases.traverse_((transfer, expected) => submit(transfer).assertEquals(Vector(expected), clue(transfer)))
      // None of them was recorded, so the hold is still live and its ID still free.
      _ <- debits(pair).assertEquals((100L, 0L))
      _ <- submit(plain).assertEquals(Vector(Created))
    yield ()

  test("amounts past 128 bits overflow, each balance in TigerBeetle's order"):
    val huge = U128(-1L, -2L)
    def plain(pair: Pair, amount: U128) =
      Transfer(TransferId(U128(0xf00dL, next())), pair.debit, pair.credit, amount, TransferId.Zero, AccountLedger, AuthorizationCode, Set.empty, 0)
    def hold(pair: Pair, amount: U128) = authorization(key(), pair).copy(amount = amount)
    for
      held <- accounts()
      _ <- submit(hold(held, huge)).assertEquals(Vector(Created))
      _ <- submit(hold(held, U128.of(2))).assertEquals(Vector(OverflowsDebitsPending))
      _ <- submit(plain(held, U128.of(2))).assertEquals(Vector(OverflowsDebits))
      posted <- accounts()
      _ <- submit(plain(posted, huge)).assertEquals(Vector(Created))
      _ <- submit(plain(posted, U128.of(2))).assertEquals(Vector(OverflowsDebitsPosted))
      creditHeld <- accounts()
      _ <- submit(hold(Pair(creditHeld.debit, held.credit), U128.of(2))).assertEquals(Vector(OverflowsCreditsPending))
      _ <- submit(plain(Pair(creditHeld.debit, posted.credit), U128.of(2))).assertEquals(Vector(OverflowsCreditsPosted))
      _ <- submit(plain(Pair(creditHeld.debit, held.credit), U128.of(2))).assertEquals(Vector(OverflowsCredits))
    yield ()

  test("setting up an account again with the same fields changes nothing, and with others it fails"):
    val id = newId()
    for
      _ <- ledger.createAccounts(Vector(account(id)))
      _ <- ledger.createAccounts(Vector(account(id)))
      _ <- ledger.createAccounts(Vector(account(id).copy(code = AccountCode + 1))).attempt.map(_.isLeft).assertEquals(true)
      _ <- ledger.createAccounts(Vector(account(id).copy(debitsMustNotExceedCredits = true))).attempt.map(_.isLeft).assertEquals(true)
      _ <- ledger.createAccounts(Vector(account(newId()).copy(code = 0))).attempt.map(_.isLeft).assertEquals(true)
      _ <- ledger.createAccounts(Vector(account(newId(), 0))).attempt.map(_.isLeft).assertEquals(true)
      _ <- balances(id).map(_.id).assertEquals(id)
      _ <- ledger.lookupAccounts(Vector(newId())).assertEquals(Vector.empty)
    yield ()

  test("a closed account refuses a post and a new hold, burning their IDs, and still lets a void release a hold"):
    val (posted, voided, late) = (key(), key(), key())
    for
      pair <- accounts()
      _ <- submit(authorization(posted, pair), authorization(voided, pair)).assertEquals(Vector(Created, Created))
      _ <- ledger.closeAccount(pair.debit, pair.credit, AccountLedger, AccountCode)
      _ <- submit(post(posted, CaptureAmount.Full)).assertEquals(Vector(DebitAccountAlreadyClosed))
      _ <- submit(post(posted, CaptureAmount.Full)).assertEquals(Vector(IdAlreadyFailed))
      _ <- submit(authorization(late, pair)).assertEquals(Vector(DebitAccountAlreadyClosed))
      _ <- submit(authorization(late, pair)).assertEquals(Vector(IdAlreadyFailed))
      _ <- cancel(voided).assertEquals(Vector(ExistsWithDifferentFlags, Created))
      _ <- debits(pair).assertEquals((100L, 0L))
    yield ()

  // The capture table of README section 8, each row replayed once. Timeouts, expiry and posts.

  test("a live hold: a full capture posts it all, and a replay returns exists"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(Created))
      _ <- debits(pair).assertEquals((0L, 100L))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(Exists))
      _ <- debits(pair).assertEquals((0L, 100L))
    yield ()

  test("a live hold: a partial capture posts its amount and releases the rest, and a replay returns exists"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- submit(post(k, CaptureAmount.Partial(40))).assertEquals(Vector(Created))
      _ <- debits(pair).assertEquals((0L, 40L))
      _ <- balances(pair.credit).map(b => (b.creditsPending.lo, b.creditsPosted.lo)).assertEquals((0L, 40L))
      _ <- submit(post(k, CaptureAmount.Partial(40))).assertEquals(Vector(Exists))
      _ <- debits(pair).assertEquals((0L, 40L))
    yield ()

  test("a post of 0 posts nothing and releases everything"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- submit(post(k, CaptureAmount.Full).copy(amount = U128.Zero)).assertEquals(Vector(Created))
      _ <- debits(pair).assertEquals((0L, 0L))
    yield ()

  test("a capture that meets its own earlier capture with a different amount returns exists_with_different_amount"):
    val (partial, full) = (key(), key())
    for
      pair <- accounts()
      _ <- submit(authorization(partial, pair), authorization(full, pair)).assertEquals(Vector(Created, Created))
      _ <- submit(post(partial, CaptureAmount.Partial(40))).assertEquals(Vector(Created))
      _ <- submit(post(partial, CaptureAmount.Partial(50))).assertEquals(Vector(ExistsWithDifferentAmount))
      _ <- submit(post(partial, CaptureAmount.Full)).assertEquals(Vector(ExistsWithDifferentAmount))
      _ <- submit(post(full, CaptureAmount.Full)).assertEquals(Vector(Created))
      _ <- submit(post(full, CaptureAmount.Partial(40))).assertEquals(Vector(ExistsWithDifferentAmount))
      // The full amount, spelled out, is what was recorded.
      _ <- submit(post(full, CaptureAmount.Partial(100))).assertEquals(Vector(Exists))
    yield ()

  test("a hold voided by a cancel: the capture finds it voided, and so does its replay"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- cancel(k).assertEquals(Vector(ExistsWithDifferentFlags, Created))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(PendingTransferAlreadyVoided))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(PendingTransferAlreadyVoided))
    yield ()

  test("a hold past its timeout: the capture finds it expired, and so does its replay"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair, timeout = OneSecond)).assertEquals(Vector(Created))
      _ <- ledger.passTime(PastOneSecond)
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(PendingTransferExpired))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(PendingTransferExpired))
    yield ()

  test("a hold smaller than the capture: the failure is not recorded, so a corrected capture reuses P"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- submit(post(k, CaptureAmount.Partial(101))).assertEquals(Vector(ExceedsPendingTransferAmount))
      _ <- submit(post(k, CaptureAmount.Partial(101))).assertEquals(Vector(ExceedsPendingTransferAmount))
      _ <- debits(pair).assertEquals((100L, 0L))
      _ <- submit(post(k, CaptureAmount.Partial(100))).assertEquals(Vector(Created))
      _ <- debits(pair).assertEquals((0L, 100L))
    yield ()

  test("a capture of a fence finds it not pending, and so does its replay"):
    val k = key()
    for
      _ <- accounts()
      _ <- submit(Transfers.fence(a(k), cfg)).assertEquals(Vector(Created))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(PendingTransferNotPending))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(PendingTransferNotPending))
    yield ()

  test("a capture with no hold at all burns P, so a resend returns id_already_failed"):
    val k = key()
    for
      pair <- accounts()
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(PendingTransferNotFound))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(IdAlreadyFailed))
      // Burning P does not burn A.
      _ <- submit(authorization(k, pair)).assertEquals(Vector(Created))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(IdAlreadyFailed))
    yield ()

  test("a capture whose hold failed with a transient error finds no hold"):
    val k = key()
    for
      pair <- accounts(debitLimited = true)
      _ <- submit(authorization(k, pair)).assertEquals(Vector(ExceedsCredits))
      _ <- submit(post(k, CaptureAmount.Full)).assertEquals(Vector(PendingTransferNotFound))
    yield ()

  // Expiry.

  test("a hold's money comes back after its timeout, within the expiry lag, and the hold still returns exists"):
    val k = key()
    def untilReleased(pair: Pair): IO[Unit] =
      debits(pair).flatMap((pending, _) => if pending == 0 then IO.unit else ledger.passTime(10.millis) >> untilReleased(pair))
    for
      pair <- accounts()
      start <- ledger.elapsed
      _ <- submit(authorization(k, pair, timeout = OneSecond)).assertEquals(Vector(Created))
      _ <- debits(pair).assertEquals((100L, 0L))
      _ <- untilReleased(pair)
      released <- ledger.elapsed
      took = released - start
      _ <- IO.println(s"${getClass.getSimpleName}: a 1 s hold was released after ${took.toMillis} ms")
      _ = assert(took >= 900.millis, s"released too early: $took")
      _ = assert(took < 3.seconds, s"released too late: $took")
      _ <- debits(pair).assertEquals((0L, 0L))
      _ <- submit(authorization(k, pair, timeout = OneSecond)).assertEquals(Vector(Exists))
      _ <- debits(pair).assertEquals((0L, 0L))
    yield ()

  // Scans by timestamp.

  private def scanAll(after: Long, limit: Int): IO[Vector[LedgerTransfer]] =
    ledger
      .scan(after, limit)
      .flatMap: page =>
        if page.isEmpty then IO.pure(page) else scanAll(page.last.timestamp, limit).map(page ++ _)

  test("a scan after a cursor returns transfers in timestamp order, as recorded, with failures and replays left out"):
    val (k, voided, posted, failed, partial) = (key(), key(), key(), key(), key())
    for
      start <- scanAll(0, 8189).map(_.lastOption.fold(0L)(_.timestamp))
      limited <- accounts(debitLimited = true)
      pair <- accounts()
      hold = authorization(k, pair)
      _ <- submit(hold).assertEquals(Vector(Created))
      _ <- submit(hold).assertEquals(Vector(Exists))
      _ <- submit(authorization(failed, limited)).assertEquals(Vector(ExceedsCredits))
      _ <- submit(authorization(voided, pair)).assertEquals(Vector(Created))
      _ <- cancel(voided).assertEquals(Vector(ExistsWithDifferentFlags, Created))
      _ <- submit(authorization(posted, pair), post(posted, CaptureAmount.Full)).assertEquals(Vector(Created, Created))
      _ <- submit(authorization(partial, pair), post(partial, CaptureAmount.Partial(40))).assertEquals(Vector(Created, Created))
      fence = key()
      _ <- submit(Transfers.fence(a(fence), cfg)).assertEquals(Vector(Created))
      scanned <- scanAll(start, 3)
      inherited = (id: TransferId, pendingId: TransferId, flag: TransferFlag, amount: Long) =>
        Transfer(id, pair.debit, pair.credit, U128.of(amount), pendingId, AccountLedger, AuthorizationCode, Set(flag), 0)
      _ = assertEquals(
        scanned.map(_.transfer),
        Vector(
          hold,
          authorization(voided, pair),
          inherited(Ids.void(a(voided)), a(voided), TransferFlag.VoidPendingTransfer, 100),
          authorization(posted, pair),
          inherited(Ids.post(a(posted)), a(posted), TransferFlag.PostPendingTransfer, 100),
          authorization(partial, pair),
          inherited(Ids.post(a(partial)), a(partial), TransferFlag.PostPendingTransfer, 40),
          Transfers.fence(a(fence), cfg),
        ),
      )
      _ = assert(scanned.map(_.timestamp).sliding(2).forall(w => w.size < 2 || w(0) < w(1)), scanned.map(_.timestamp))
      _ = assert(scanned.forall(_.cluster == Period(0)))
      _ <- ledger.scan(scanned.last.timestamp, 10).assertEquals(Vector.empty)
    yield ()

  test("a scan does not show an expiry: the hold stays as it was recorded"):
    val k = key()
    for
      start <- scanAll(0, 8189).map(_.lastOption.fold(0L)(_.timestamp))
      pair <- accounts()
      hold = authorization(k, pair, timeout = OneSecond)
      _ <- submit(hold).assertEquals(Vector(Created))
      _ <- ledger.passTime(PastOneSecond)
      scanned <- scanAll(start, 10)
      _ = assertEquals(scanned.map(_.transfer), Vector(hold))
    yield ()

  test("a scan skips nothing while other calls write"):
    val writers = 4
    val perWriter = 50
    def following(after: Long, seen: Vector[LedgerTransfer], done: IO[Boolean]): IO[Vector[LedgerTransfer]] =
      ledger
        .scan(after, 16)
        .flatMap: page =>
          if page.nonEmpty then following(page.last.timestamp, seen ++ page, done)
          else done.flatMap(finished => if finished then IO.pure(seen) else ledger.passTime(1.millis) >> following(after, seen, done))
    for
      start <- scanAll(0, 8189).map(_.lastOption.fold(0L)(_.timestamp))
      pair <- accounts()
      keys = Vector.fill(writers)(Vector.fill(perWriter)(key()))
      finished <- cats.effect.Ref[IO].of(false)
      write = keys.parTraverse_(mine => mine.traverse_(k => submit(authorization(k, pair)).assertEquals(Vector(Created)))) >> finished.set(true)
      scanned <- (write, following(start, Vector.empty, finished.get)).parMapN((_, seen) => seen)
      _ = assertEquals(scanned.map(_.id).toSet, keys.flatten.map(a).toSet)
      _ = assertEquals(scanned.size, writers * perWriter)
      _ = assert(scanned.map(_.timestamp).sliding(2).forall(w => w(0) < w(1)))
    yield ()

  test("calls in flight together each get their own statuses, in their own order"):
    val calls = 64
    for
      pair <- accounts()
      limited <- accounts(debitLimited = true)
      keys = Vector.fill(calls)(key())
      results <- keys.parTraverse(k => submit(authorization(k, pair), authorization(k, pair), authorization(key(), limited)))
      _ = results.foreach(r => assertEquals(r, Vector(Created, Exists, ExceedsCredits)))
    yield ()
