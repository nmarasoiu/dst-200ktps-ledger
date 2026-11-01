package ledger.core

/** A caller's connection to an edge instance: the caller API's handle, on which the answer goes back (README
  * section 9). The interpreter chooses it, unique for the instance's life. An opaque `Long`: stored as a primitive.
  */
opaque type Connection = Long

object Connection:
  def apply(n: Long): Connection = n
  extension (c: Connection) def value: Long = c
  given CanEqual[Connection, Connection] = CanEqual.derived

/** The command log port (README section 11, ports): one Kafka topic, keyed by `A`. The edge appends commands and
  * outcomes and follows the producer's metadata; the resolver reads the topic as a consumer, appends outcomes and
  * commits offsets. fs2-kafka is the real adapter and the simulator has its own; one contract suite holds both to
  * these types (README section 14, stage 4).
  */
object CommandLog:

  /** Append a record to the partition of its key, `A`, and wait for the in-sync replicas (README section 4). */
  final case class Append(record: Record) derives CanEqual:
    def partition(partitions: Int): Partition = Partitioner.partition(record.command.a, partitions)

  /** The reply to an append. */
  enum Written derives CanEqual:
    case Acknowledged
    /** Not acknowledged, for example after a timeout: the record may still land (README section 5). */
    case Failed

  /** The partitions the producer's metadata shows without a leader (README section 8, failover). */
  final case class Leaders(leaderless: Set[Partition]) derives CanEqual

  /** What the consumer tells the resolver. */
  enum Delivery derives CanEqual:
    /** The resolver reads the partition again from its committed offset. */
    case Assigned(partition: Partition, committed: Offset)
    case Revoked(partition: Partition)
    /** A record, with the append time its partition leader stamped on it (README section 4, command log rules). */
    case Read(partition: Partition, offset: Offset, appended: LogTime, record: Record)
    /** A poll found nothing past the last record read: the end of the partition (README section 6). The consumer
      * keeps polling an idle partition, since the resolver judges an idle partition's commands only when it hears
      * this (README section 8, stragglers).
      */
    case ReachedEnd(partition: Partition)

  /** Commit the offset to read again from. The group coordinator rejects it once a rebalance has moved the
    * partition away, and the resolver needs no reply (README section 5, zombie resolver).
    */
  final case class Commit(partition: Partition, next: Offset) derives CanEqual

/** The ledger port: TigerBeetle, one cluster per period (README section 4). The Java client is the real adapter. */
object Ledger:

  /** One `create_transfers` call to the cluster of a period, through the client the process shares. The client
    * packs the events of calls made while its previous request is in flight into its next request (README
    * section 6).
    */
  final case class Submit(cluster: Period, transfers: Vector[Transfer]) derives CanEqual

  /** The reply to a call. */
  enum Result derives CanEqual:
    /** One status per transfer, in order. */
    case Returned(statuses: Vector[TbStatus])
    /** The call failed without statuses, so its effect is unknown. */
    case Failed

  /** The ledger feed: up to `limit` transfers of a cluster with a timestamp after `after`, in timestamp order, as
    * `LedgerTransfer`s. The scan after each run reads it to the end, and so will the tailer of phase E (README
    * section 12, gap 4). No machine reads it.
    */
  final case class Scan(cluster: Period, after: Long, limit: Int) derives CanEqual

  /** Accounts to set up in a cluster, such as the two fence accounts (README section 8, the cost). Setting up an
    * account again with the same fields changes nothing. No machine does this.
    */
  final case class CreateAccounts(cluster: Period, accounts: Vector[NewAccount]) derives CanEqual

  final case class NewAccount(
      id: AccountId,
      ledger: Int,
      code: Int,
      debitsMustNotExceedCredits: Boolean = false,
      creditsMustNotExceedDebits: Boolean = false,
  ) derives CanEqual

  /** Read the balances of accounts: how a hold's expiry shows from outside, since a scan cannot see it (README
    * section 12, gap 4). No machine does this.
    */
  final case class LookupAccounts(cluster: Period, ids: Vector[AccountId]) derives CanEqual

  /** Close an account, as an operator might under a live hold (README section 11, coverage marks). No machine does
    * this.
    */
  final case class CloseAccount(cluster: Period, id: AccountId, other: AccountId, ledger: Int, code: Int) derives CanEqual

  final case class Balances(id: AccountId, debitsPending: U128, debitsPosted: U128, creditsPending: U128, creditsPosted: U128)
      derives CanEqual
