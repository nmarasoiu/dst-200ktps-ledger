package ledger.core

/** One run's history: the one format the simulator and the stage 6 workload driver write and every invariant
  * reads (README section 11, one history for every layer). It has three sources: what each caller sent and
  * received, what the edges and the resolver wrote to the command log, and what TigerBeetle holds. The simulator
  * adds a fourth, what each edge instance saw of each request by its own clock, for the deadline property.
  *
  * `at` times are on the history's one clock. TigerBeetle's timestamps stay in its own nanoseconds, per cluster.
  */
final case class History(
    caller: Vector[CallerRecord],
    log: Vector[LogRecord],
    edges: Vector[EdgeRecord],
    ledger: LedgerSnapshot,
    /** When the simulator healed every fault, or the fault scenario ended; None if there were no faults. */
    faultsStoppedAt: Option[HistoryTime],
    endedAt: HistoryTime,
) derives CanEqual

enum CallerRecord derives CanEqual:
  case Sent(at: HistoryTime, request: Request)
  case Received(at: HistoryTime, request: Request, reply: Reply)
  /** A connection error, or no answer at all. */
  case Lost(at: HistoryTime, request: Request)

  def at: HistoryTime
  def request: Request

/** A command log record. `at` is when its writer had it acknowledged, in history time; in real runs, its append
  * time.
  */
final case class LogRecord(partition: Partition, offset: Offset, at: HistoryTime, record: Record) derives CanEqual

/** What one edge instance saw of one request, by its own clock: when the request arrived and, unless the instance
  * crashed first, when and how it was answered. `at` is the arrival in history time. The simulator keeps these;
  * real runs have none until stage 6 settles how edges export them.
  */
final case class EdgeRecord(instance: Int, at: HistoryTime, request: Request, arrived: EdgeTime, answered: Option[(EdgeTime, Reply)])
    derives CanEqual

/** A transfer from the scan of one cluster, with TigerBeetle's timestamp in nanoseconds. */
final case class LedgerTransfer(cluster: Period, transfer: Transfer, timestamp: Long) derives CanEqual:
  def id: TransferId = transfer.id

  /** When TigerBeetle expires the hold, if it is a pending transfer with a timeout. */
  def expiresAt: Option[Long] =
    if transfer.isPending && transfer.timeoutSeconds > 0 then Some(timestamp + transfer.timeoutSeconds * 1_000_000_000L)
    else None

final case class LedgerAccount(
    cluster: Period,
    id: AccountId,
    debitsMustNotExceedCredits: Boolean,
    creditsMustNotExceedDebits: Boolean,
) derives CanEqual

/** What TigerBeetle holds at the end of a run. `scannedAt` is each cluster's timestamp at the scan. `expiries` are
  * the holds TigerBeetle expired, with when, if the run observed them, as the simulator does; a scan cannot see
  * them, so real runs infer them from each hold's timestamp and timeout (README section 12, gap 4).
  */
final case class LedgerSnapshot(
    transfers: Vector[LedgerTransfer],
    accounts: Vector[LedgerAccount],
    scannedAt: Map[Period, Long],
    expiries: Option[Map[TransferId, Long]],
) derives CanEqual:
  private lazy val byId: Map[TransferId, Vector[LedgerTransfer]] = transfers.groupBy(_.id)
  private lazy val byPending: Map[TransferId, Vector[LedgerTransfer]] =
    transfers.filter(t => t.transfer.isPost || t.transfer.isVoid).groupBy(_.transfer.pendingId)

  def withId(id: TransferId): Vector[LedgerTransfer] = byId.getOrElse(id, Vector.empty)
  def pending(a: TransferId): Option[LedgerTransfer] = withId(a).find(_.transfer.isPending)
  def posts(a: TransferId): Vector[LedgerTransfer] = byPending.getOrElse(a, Vector.empty).filter(_.transfer.isPost)
  def voids(a: TransferId): Vector[LedgerTransfer] = byPending.getOrElse(a, Vector.empty).filter(_.transfer.isVoid)
  def duplicateIds: Map[TransferId, Vector[LedgerTransfer]] = byId.filter(_._2.size > 1)
  def closedMoreThanOnce: Map[TransferId, Vector[LedgerTransfer]] = byPending.filter(_._2.size > 1)

  /** Whether a hold had expired by the scan: observed, or inferred from its timeout. */
  def expired(hold: LedgerTransfer): Boolean = expiries match
    case Some(seen) => seen.contains(hold.id)
    case None => hold.expiresAt.exists(at => scannedAt.get(hold.cluster).exists(at <= _))

  /** The amount a post moved: the full hold when it asked for the maximum (README section 6). */
  def postedAmount(post: LedgerTransfer, hold: LedgerTransfer): U128 =
    if post.transfer.amount.isMax then hold.transfer.amount else post.transfer.amount

/** A broken invariant or property, named as README section 11 states it. */
final case class Violation(property: String, detail: String) derives CanEqual
