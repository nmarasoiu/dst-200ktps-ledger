package ledger.sim

import ledger.core.*
import ledger.core.TbStatus.*
import scala.collection.mutable

/** One cluster's state machine, as `src/state_machine.zig` runs it, for the fields the pipeline sets: IDs, fences,
  * voids, posts, balance limits, pending timeouts and queries by timestamp (README section 11, simulated adapters). The
  * ledger contract suite holds it to the real system. Checks run in the source's order, so a transfer wrong in two
  * ways gets the same code. It knows nothing of delays or lost replies, which are the event loop's, and its time is
  * given by the caller: TigerBeetle's own clock, in nanoseconds.
  *
  * Lookups go through hash maps, but nothing iterates over one, so a seed's run does not depend on their order.
  */
final class SimTigerBeetle(cluster: Period):
  import SimTigerBeetle.*

  private val accounts = mutable.HashMap.empty[AccountId, Account]
  private val transfers = mutable.HashMap.empty[TransferId, LedgerTransfer]
  private val orphaned = mutable.HashSet.empty[TransferId]
  private val closings = mutable.HashMap.empty[TransferId, Closing]
  private val expiring = mutable.TreeSet.empty[(Long, Long, TransferId)]
  private val timeline = mutable.ArrayBuffer.empty[LedgerTransfer]
  private val accountOrder = mutable.ArrayBuffer.empty[AccountId]
  private val expired = mutable.ArrayBuffer.empty[(TransferId, Long)]
  private var commitTimestamp = 0L

  /** Setting up an account again with the same fields changes nothing. Anything else is refused, as the adapter does.
    */
  def createAccounts(now: Long, requested: Vector[Ledger.NewAccount]): Unit =
    commitTimestamp = math.max(now, commitTimestamp + requested.size)
    requested.foreach: a =>
      require(a.ledger != 0 && a.code != 0, s"accounts not set up: $a")
      accounts.get(a.id) match
        case Some(existing) => require(existing.fields == a, s"accounts not set up: $a differs from ${existing.fields}")
        case None =>
          accounts(a.id) = Account(a)
          accountOrder += a.id

  /** Refuses new movements on the account, apart from voids. */
  def closeAccount(id: AccountId): Unit = accounts(id).closed = true

  /** One request's events, in order: later events see what earlier ones did, and a transient failure burns its ID. */
  def createTransfers(now: Long, batch: Vector[Transfer]): Vector[TbStatus] =
    val last = math.max(now, commitTimestamp + batch.size)
    val statuses = batch.zipWithIndex.map: (t, i) =>
      val status = create(last - batch.size + i + 1, t)
      status.group match
        case TbGroup.Transient(_) => orphaned += t.id
        case _ =>
      status
    commitTimestamp = last
    statuses

  /** Expires up to one batch of holds past their timeout. The primary runs this after a commit, or every 100 ms when
    * idle (README section 6), so the lag between a hold's timeout and its release belongs to whoever calls this. Until
    * then the hold is refused by time alone.
    */
  def pulse(now: Long): Unit =
    val due = expiring.iterator.takeWhile(_._1 <= now).take(ExpiryBatch).toVector
    val last = math.max(now, commitTimestamp + due.size)
    due.zipWithIndex.foreach:
      case (entry @ (_, _, id), i) =>
        val hold = transfers(id).transfer
        expiring -= entry
        closings(id) = Closing.Expired
        release(hold)
        expired += id -> (last - due.size + i + 1)
    commitTimestamp = last

  /** Up to `limit` transfers after `after`, in timestamp order. `exists` adds nothing and a failure records nothing. */
  def scan(after: Long, limit: Int): Vector[LedgerTransfer] =
    timeline.view.drop(firstAfter(after)).take(math.min(limit, ScanLimit)).toVector

  def lookupAccounts(ids: Vector[AccountId]): Vector[Ledger.Balances] =
    ids.flatMap(accounts.get).map(_.balances)

  /** What the run's history reads at its end. The expiries are observed here, which a real scan cannot do. */
  def snapshot: LedgerSnapshot =
    LedgerSnapshot(
      timeline.toVector,
      accountOrder.toVector.map: id =>
        val a = accounts(id).fields
        LedgerAccount(cluster, id, a.debitsMustNotExceedCredits, a.creditsMustNotExceedDebits),
      Map(cluster -> commitTimestamp),
      Some(expired.toMap),
    )

  private def firstAfter(after: Long): Int =
    var (low, high) = (0, timeline.size)
    while low < high do
      val middle = (low + high) >>> 1
      if timeline(middle).timestamp <= after then low = middle + 1 else high = middle
    low

  /** `create_transfer`. */
  private def create(timestamp: Long, t: Transfer): TbStatus =
    if t.id.value.isZero then IdMustNotBeZero
    else if t.id.value.isMax then IdMustNotBeIntMax
    else
      transfers.get(t.id) match
        case Some(existing) => exists(t, existing.transfer)
        case None if orphaned(t.id) => IdAlreadyFailed
        case None if t.isPost || t.isVoid => postOrVoid(timestamp, t)
        case None => plain(timestamp, t)

  private def plain(timestamp: Long, t: Transfer): TbStatus =
    val (debit, credit) = (t.debit.value, t.credit.value)
    if debit.isZero then DebitAccountIdMustNotBeZero
    else if debit.isMax then DebitAccountIdMustNotBeIntMax
    else if credit.isZero then CreditAccountIdMustNotBeZero
    else if credit.isMax then CreditAccountIdMustNotBeIntMax
    else if t.debit == t.credit then AccountsMustBeDifferent
    else if !t.pendingId.value.isZero then PendingIdMustBeZero
    else if !t.isPending && t.timeoutSeconds != 0 then TimeoutReservedForPendingTransfer
    else if t.ledger == 0 then LedgerMustNotBeZero
    else if t.code == 0 then CodeMustNotBeZero
    else
      (accounts.get(t.debit), accounts.get(t.credit)) match
        case (None, _) => DebitAccountNotFound
        case (_, None) => CreditAccountNotFound
        case (Some(dr), Some(cr)) =>
          if dr.fields.ledger != cr.fields.ledger then AccountsMustHaveTheSameLedger
          else if t.ledger != dr.fields.ledger then TransferMustHaveTheSameLedgerAsAccounts
          else if dr.closed then DebitAccountAlreadyClosed
          else if cr.closed then CreditAccountAlreadyClosed
          else
            val amount = t.amount.toBigInt
            overflowOrLimit(t, amount, dr, cr).getOrElse:
              record(LedgerTransfer(cluster, t, timestamp))
              if t.isPending then
                dr.debitsPending += amount
                cr.creditsPending += amount
                if t.timeoutSeconds > 0 then expiring += ((timestamp + t.timeoutSeconds * NanosPerSecond, timestamp, t.id))
              else
                dr.debitsPosted += amount
                cr.creditsPosted += amount
              Created

  private def overflowOrLimit(t: Transfer, amount: BigInt, dr: Account, cr: Account): Option[TbStatus] =
    def overflows(balance: BigInt) = amount + balance > MaxAmount
    if t.isPending && overflows(dr.debitsPending) then Some(OverflowsDebitsPending)
    else if t.isPending && overflows(cr.creditsPending) then Some(OverflowsCreditsPending)
    else if overflows(dr.debitsPosted) then Some(OverflowsDebitsPosted)
    else if overflows(cr.creditsPosted) then Some(OverflowsCreditsPosted)
    else if overflows(dr.debitsPending + dr.debitsPosted) then Some(OverflowsDebits)
    else if overflows(cr.creditsPending + cr.creditsPosted) then Some(OverflowsCredits)
    else if dr.fields.debitsMustNotExceedCredits && dr.debitsPending + dr.debitsPosted + amount > dr.creditsPosted then Some(ExceedsCredits)
    else if cr.fields.creditsMustNotExceedDebits && cr.creditsPending + cr.creditsPosted + amount > cr.debitsPosted then Some(ExceedsDebits)
    else None

  /** `post_or_void_pending_transfer`. */
  private def postOrVoid(timestamp: Long, t: Transfer): TbStatus =
    if (t.isPost && t.isVoid) || t.isPending then FlagsAreMutuallyExclusive
    else if t.pendingId.value.isZero then PendingIdMustNotBeZero
    else if t.pendingId.value.isMax then PendingIdMustNotBeIntMax
    else if t.pendingId == t.id then PendingIdMustBeDifferent
    else if t.timeoutSeconds != 0 then TimeoutReservedForPendingTransfer
    else
      transfers.get(t.pendingId).map(_.transfer) match
        case None => PendingTransferNotFound
        case Some(p) if !p.isPending => PendingTransferNotPending
        case Some(p) => closeHold(timestamp, t, p)

  private def closeHold(timestamp: Long, t: Transfer, p: Transfer): TbStatus =
    val heldAmount = p.amount.toBigInt
    val actual =
      if t.isVoid then (if t.amount.isZero then heldAmount else t.amount.toBigInt)
      else if t.amount.isMax then heldAmount
      else t.amount.toBigInt
    if !t.debit.value.isZero && t.debit != p.debit then PendingTransferHasDifferentDebitAccountId
    else if !t.credit.value.isZero && t.credit != p.credit then PendingTransferHasDifferentCreditAccountId
    else if t.ledger != 0 && t.ledger != p.ledger then PendingTransferHasDifferentLedger
    else if t.code != 0 && t.code != p.code then PendingTransferHasDifferentCode
    else if actual > heldAmount then ExceedsPendingTransferAmount
    else if t.isVoid && actual < heldAmount then PendingTransferHasDifferentAmount
    else
      closings.get(p.id) match
        case Some(Closing.Posted) => PendingTransferAlreadyPosted
        case Some(Closing.Voided) => PendingTransferAlreadyVoided
        case Some(Closing.Expired) => PendingTransferExpired
        case None =>
          val heldAt = transfers(p.id).timestamp
          val expiresAt = heldAt + p.timeoutSeconds * NanosPerSecond
          if p.timeoutSeconds > 0 && expiresAt <= timestamp then PendingTransferExpired
          else if t.isPost && accounts(p.debit).closed then DebitAccountAlreadyClosed
          else if t.isPost && accounts(p.credit).closed then CreditAccountAlreadyClosed
          else
            if p.timeoutSeconds > 0 then expiring -= ((expiresAt, heldAt, p.id))
            closings(p.id) = if t.isPost then Closing.Posted else Closing.Voided
            release(p)
            if t.isPost then
              accounts(p.debit).debitsPosted += actual
              accounts(p.credit).creditsPosted += actual
            val recorded = Transfer(t.id, p.debit, p.credit, U128.fromBigInt(actual), t.pendingId, p.ledger, p.code, t.flags, 0)
            record(LedgerTransfer(cluster, recorded, timestamp))
            Created

  private def release(hold: Transfer): Unit =
    accounts(hold.debit).debitsPending -= hold.amount.toBigInt
    accounts(hold.credit).creditsPending -= hold.amount.toBigInt

  private def record(t: LedgerTransfer): Unit =
    transfers(t.id) = t
    timeline += t
    commitTimestamp = t.timestamp

  /** `create_transfer_exists`: flags first, since they change what the rest of the comparison means. */
  private def exists(t: Transfer, e: Transfer): TbStatus =
    if t.flags != e.flags then ExistsWithDifferentFlags
    else if t.pendingId != e.pendingId then ExistsWithDifferentPendingId
    else if t.timeoutSeconds != e.timeoutSeconds then ExistsWithDifferentTimeout
    else if t.isPost || t.isVoid then postOrVoidExists(t, e, transfers(t.pendingId).transfer)
    else if t.debit != e.debit then ExistsWithDifferentDebitAccountId
    else if t.credit != e.credit then ExistsWithDifferentCreditAccountId
    else if t.amount != e.amount then ExistsWithDifferentAmount
    else if t.ledger != e.ledger then ExistsWithDifferentLedger
    else if t.code != e.code then ExistsWithDifferentCode
    else Exists

  /** `post_or_void_pending_transfer_exists`. */
  private def postOrVoidExists(t: Transfer, e: Transfer, p: Transfer): TbStatus =
    val asksForAll = if t.isVoid then t.amount.isZero else t.amount.isMax
    val requested = if asksForAll then p.amount else t.amount
    if !t.debit.value.isZero && t.debit != e.debit then ExistsWithDifferentDebitAccountId
    else if !t.credit.value.isZero && t.credit != e.credit then ExistsWithDifferentCreditAccountId
    else if requested != e.amount then ExistsWithDifferentAmount
    else if t.ledger != 0 && t.ledger != e.ledger then ExistsWithDifferentLedger
    else if t.code != 0 && t.code != e.code then ExistsWithDifferentCode
    else Exists

object SimTigerBeetle:
  /** Each pulse expires up to one batch, and a query returns up to one batch (README section 6). */
  val ExpiryBatch = 8189
  val ScanLimit = 8189

  private val NanosPerSecond = 1_000_000_000L
  private val MaxAmount = U128.Max.toBigInt

  private enum Closing derives CanEqual:
    case Posted, Voided, Expired

  private final class Account(val fields: Ledger.NewAccount):
    var debitsPending = BigInt(0)
    var debitsPosted = BigInt(0)
    var creditsPending = BigInt(0)
    var creditsPosted = BigInt(0)
    var closed = false

    def balances: Ledger.Balances = Ledger.Balances(
      fields.id,
      U128.fromBigInt(debitsPending),
      U128.fromBigInt(debitsPosted),
      U128.fromBigInt(creditsPending),
      U128.fromBigInt(creditsPosted),
    )
