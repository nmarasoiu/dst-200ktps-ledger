package ledger.shell

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.tigerbeetle.{
  AccountBatch,
  AccountFlags,
  Client,
  CreateAccountStatus,
  CreateTransferResultBatch,
  IdBatch,
  QueryFilter,
  TransferBatch,
  TransferFlags,
  UInt128,
}
import java.util.concurrent.CompletableFuture
import ledger.core.*
import scala.util.control.NonFatal

/** The ledger port's real adapter (README section 11, ports): one Java client per cluster, shared by the whole
  * process, so a client packs the events of every request in flight into its next request (README section 6).
  */
final class TigerBeetle private (clients: Map[Period, Client]):
  import TigerBeetle.*

  /** Never fails: a call that gets no statuses has an unknown effect, whatever went wrong. */
  def submit(call: Ledger.Submit): IO[Ledger.Result] =
    awaited(clients(call.cluster).createTransfersAsync(encode(call.transfers)))
      .map(results => Ledger.Result.Returned(decodeStatuses(results)))
      .recover { case NonFatal(_) => Ledger.Result.Failed }

  /** An account's closing transfer is an operator's, not the pipeline's, so the feed leaves it out. A page of nothing
    * else reads on, since an empty page means the end.
    */
  def scan(call: Ledger.Scan): IO[Vector[LedgerTransfer]] =
    val filter = new QueryFilter
    filter.setTimestampMin(call.after + 1)
    filter.setLimit(call.limit)
    awaited(clients(call.cluster).queryTransfersAsync(filter)).flatMap: batch =>
      val (kept, last) = decodeTransfers(call.cluster, batch)
      last match
        case Some(timestamp) if kept.isEmpty => scan(call.copy(after = timestamp))
        case _ => IO.pure(kept)

  def createAccounts(call: Ledger.CreateAccounts): IO[Unit] =
    val batch = new AccountBatch(call.accounts.size)
    call.accounts.foreach: a =>
      batch.add()
      batch.setId(a.id.value.lo, a.id.value.hi)
      batch.setLedger(a.ledger)
      batch.setCode(a.code)
      val debits = if a.debitsMustNotExceedCredits then AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS else AccountFlags.NONE
      val credits = if a.creditsMustNotExceedDebits then AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS else AccountFlags.NONE
      batch.setFlags(debits | credits)
    awaited(clients(call.cluster).createAccountsAsync(batch)).flatMap: results =>
      val statuses = Vector.newBuilder[CreateAccountStatus]
      while results.next() do statuses += results.getStatus
      statuses.result().filterNot(ok).toList match
        case Nil => IO.unit
        case bad => IO.raiseError(IllegalStateException(s"accounts not set up: $bad"))

  /** A zero-amount pending transfer that closes the account, with no timeout, so it stays closed. */
  def closeAccount(call: Ledger.CloseAccount): IO[Unit] =
    val batch = new TransferBatch(1)
    batch.add()
    val id = java.util.UUID.nameUUIDFromBytes(call.id.value.bytes)
    batch.setId(UInt128.asBytes(id))
    batch.setDebitAccountId(call.id.value.lo, call.id.value.hi)
    batch.setCreditAccountId(call.other.value.lo, call.other.value.hi)
    batch.setLedger(call.ledger)
    batch.setCode(call.code)
    batch.setFlags(TransferFlags.PENDING | TransferFlags.CLOSING_DEBIT)
    awaited(clients(call.cluster).createTransfersAsync(batch)).flatMap: results =>
      val statuses = decodeStatuses(results)
      if statuses == Vector(TbStatus.Created) then IO.unit
      else IO.raiseError(IllegalStateException(s"account not closed: $statuses"))

  def lookupAccounts(call: Ledger.LookupAccounts): IO[Vector[Ledger.Balances]] =
    val ids = new IdBatch(call.ids.size)
    call.ids.foreach(id => ids.add(id.value.lo, id.value.hi))
    awaited(clients(call.cluster).lookupAccountsAsync(ids)).map: found =>
      val balances = Vector.newBuilder[Ledger.Balances]
      while found.next() do
        balances += Ledger.Balances(
          AccountId(u128(found.getId(UInt128.LeastSignificant), found.getId(UInt128.MostSignificant))),
          u128(found.getDebitsPending(UInt128.LeastSignificant), found.getDebitsPending(UInt128.MostSignificant)),
          u128(found.getDebitsPosted(UInt128.LeastSignificant), found.getDebitsPosted(UInt128.MostSignificant)),
          u128(found.getCreditsPending(UInt128.LeastSignificant), found.getCreditsPending(UInt128.MostSignificant)),
          u128(found.getCreditsPosted(UInt128.LeastSignificant), found.getCreditsPosted(UInt128.MostSignificant)),
        )
      balances.result()

object TigerBeetle:

  /** A cluster's ID and its replicas' addresses. */
  final case class Cluster(id: U128, addresses: Vector[String])

  def resource(clusters: Map[Period, Cluster]): Resource[IO, TigerBeetle] =
    clusters.toVector
      .traverse: (period, cluster) =>
        Resource
          .fromAutoCloseable(IO.blocking(new Client(UInt128.asBytes(cluster.id.lo, cluster.id.hi), cluster.addresses.toArray)))
          .map(period -> _)
      .map(clients => new TigerBeetle(clients.toMap))

  /** Waits for a future the client completes. A fiber that stops waiting leaves the future alone. `IO`'s own
    * `fromCompletableFuture` would cancel it, and the client halts the JVM when it then cannot complete it
    * (`Request.endRequest`; README section 6). The event was handed over and cannot be taken back anyway.
    */
  private def awaited[A](start: => CompletableFuture[A]): IO[A] =
    IO(start).flatMap: future =>
      IO.async: resume =>
        IO:
          future.whenComplete((a, failure) => resume(if failure == null then Right(a) else Left(failure)))
          Some(IO.unit)

  private given CanEqual[CreateAccountStatus, CreateAccountStatus] = CanEqual.derived

  private def ok(status: CreateAccountStatus): Boolean =
    status == CreateAccountStatus.Created || status == CreateAccountStatus.Exists

  private def encode(transfers: Vector[Transfer]): TransferBatch =
    val batch = new TransferBatch(transfers.size)
    transfers.foreach: t =>
      batch.add()
      batch.setId(t.id.value.lo, t.id.value.hi)
      batch.setDebitAccountId(t.debit.value.lo, t.debit.value.hi)
      batch.setCreditAccountId(t.credit.value.lo, t.credit.value.hi)
      batch.setAmount(t.amount.lo, t.amount.hi)
      batch.setPendingId(t.pendingId.value.lo, t.pendingId.value.hi)
      batch.setTimeout(t.timeoutSeconds)
      batch.setLedger(t.ledger)
      batch.setCode(t.code)
      batch.setFlags(t.flags.foldLeft(TransferFlags.NONE)(_ | flagBit(_)))
    batch

  private def flagBit(flag: TransferFlag): Int = flag match
    case TransferFlag.Pending => TransferFlags.PENDING
    case TransferFlag.PostPendingTransfer => TransferFlags.POST_PENDING_TRANSFER
    case TransferFlag.VoidPendingTransfer => TransferFlags.VOID_PENDING_TRANSFER

  private val flagsByBit: Vector[(Int, TransferFlag)] = TransferFlag.values.toVector.map(f => flagBit(f) -> f)
  private val knownBits: Int = flagsByBit.map(_._1).reduce(_ | _)

  private def decodeFlags(bits: Int): Set[TransferFlag] =
    if (bits & ~knownBits) != 0 then throw IllegalStateException(s"a transfer with flags the pipeline never sets: $bits")
    flagsByBit.collect { case (bit, flag) if (bits & bit) != 0 => flag }.toSet

  private def u128(lo: Long, hi: Long): U128 = U128(hi, lo)

  private val statusByCode: Map[Long, TbStatus] = TbStatus.values.map(s => s.code -> s).toMap

  /** The core's status with the numeric code of the client's status. */
  def statusOf(code: Int): Option[TbStatus] = statusByCode.get(Integer.toUnsignedLong(code))

  private def decodeStatuses(results: CreateTransferResultBatch): Vector[TbStatus] =
    val statuses = Vector.newBuilder[TbStatus]
    while results.next() do
      val code = results.getStatus.value
      statuses += statusOf(code).getOrElse(throw IllegalStateException(s"a status the core does not know: $code"))
    statuses.result()

  /** The transfers the pipeline wrote, and the last timestamp read. */
  private def decodeTransfers(cluster: Period, batch: TransferBatch): (Vector[LedgerTransfer], Option[Long]) =
    val transfers = Vector.newBuilder[LedgerTransfer]
    var last = Option.empty[Long]
    while batch.next() do
      last = Some(batch.getTimestamp)
      if (batch.getFlags & TransferFlags.CLOSING_DEBIT) == 0 then
        val transfer = Transfer(
          TransferId(u128(batch.getId(UInt128.LeastSignificant), batch.getId(UInt128.MostSignificant))),
          AccountId(u128(batch.getDebitAccountId(UInt128.LeastSignificant), batch.getDebitAccountId(UInt128.MostSignificant))),
          AccountId(u128(batch.getCreditAccountId(UInt128.LeastSignificant), batch.getCreditAccountId(UInt128.MostSignificant))),
          u128(batch.getAmount(UInt128.LeastSignificant), batch.getAmount(UInt128.MostSignificant)),
          TransferId(u128(batch.getPendingId(UInt128.LeastSignificant), batch.getPendingId(UInt128.MostSignificant))),
          batch.getLedger,
          batch.getCode,
          decodeFlags(batch.getFlags),
          batch.getTimeout,
        )
        transfers += LedgerTransfer(cluster, transfer, batch.getTimestamp)
    (transfers.result(), last)
