package ledger.sim

import ledger.core.*
import ledger.core.CommandLog.{Commit, Delivery, Leaders, Written}
import scala.collection.immutable.SortedMap
import scala.collection.mutable

/** One topic and its one consumer group, modeling only what the design depends on (README section 11, simulated
  * adapters). The log contract suites hold it to real brokers.
  *
  * What readers see is what has landed, the high watermark: replication is not modeled, so a record either lands whole
  * or never exists. Whether a write lands, and whether its writer hears so, is the event loop's to choose: acknowledged
  * only after landing, while a write that failed may land or not (README section 5). The clock is given by the caller.
  *
  * Nothing iterates over a hash map, so a seed's run does not depend on their order.
  */
final class SimKafka(val partitions: Int, memberTimeout: Long):
  import SimKafka.*

  private val logs = Vector.fill(partitions)(mutable.ArrayBuffer.empty[Delivery.Read])
  private var withoutLeader = Set.empty[Partition]
  private var outOfSync = Set.empty[Partition]
  private val committedOffsets = mutable.HashMap.empty[Partition, Offset]
  private var members = SortedMap.empty[Long, Member]
  private var nextMember = 0L
  private val dropped = mutable.HashMap.empty[Long, Member]
  private val owners = mutable.HashMap.empty[Partition, Long]
  private val ownership = Array.fill(partitions)(0L)

  def partitionOf(record: Record): Partition = Partitioner.partition(record.command.a, partitions)

  /** Appends a record if its partition can take it, and acknowledges it. */
  def append(record: Record, appended: LogTime): Written =
    val p = partitionOf(record)
    if withoutLeader(p) || outOfSync(p) then Written.Failed
    else
      land(record, appended)
      Written.Acknowledged

  /** The record lands: readers see it from now on, whatever its writer was told. */
  def land(record: Record, appended: LogTime): Delivery.Read =
    val p = partitionOf(record)
    val read: Delivery.Read = Delivery.Read(p, Offset(logs(p.index).size.toLong), appended, record)
    logs(p.index) += read
    read

  def endOffset(p: Partition): Offset = Offset(logs(p.index).size.toLong)

  def records(p: Partition): Vector[Delivery.Read] = logs(p.index).toVector

  def committed(p: Partition): Option[Offset] = committedOffsets.get(p)

  /** The partitions without a leader, as the producer's metadata shows them. */
  def leaders: Leaders = Leaders(withoutLeader)

  def setLeader(p: Partition, present: Boolean): Unit =
    withoutLeader = if present then withoutLeader - p else withoutLeader + p

  /** Fewer replicas in sync than `min.insync.replicas`: a write fails, and does not land. */
  def setTooFewInSync(p: Partition, tooFew: Boolean): Unit =
    outOfSync = if tooFew then outOfSync + p else outOfSync - p

  /** A new member of the group: partitions move at once, and each member hears at its next poll. */
  def join(now: Long): MemberId =
    val id = nextMember
    nextMember += 1
    members = members.updated(id, new Member(now))
    rebalance()
    MemberId(id)

  /** A member gives its partitions back, as a clean close does. */
  def leave(m: MemberId): Unit =
    members = members.removed(m.value)
    rebalance()

  /** The coordinator drops the members it has not heard from, which a crashed or paused process becomes. */
  def expireMembers(now: Long): Unit =
    val quiet = members.iterator.collect { case (id, member) if now - member.polledAt >= memberTimeout => id }.toVector
    if quiet.nonEmpty then
      quiet.foreach(id => dropped(id) = members(id))
      members = members -- quiet
      rebalance()

  /** One poll: what moved since the last one, then up to `maxRecords` records from each partition in order, then
    * every partition whose end it has reached. A member the coordinator dropped joins again, as a client does.
    */
  def poll(m: MemberId, now: Long, maxRecords: Int = Int.MaxValue): Vector[Delivery] =
    val member = members.get(m.value) match
      case Some(known) => known
      case None =>
        val back = dropped.remove(m.value).getOrElse(new Member(now))
        members = members.updated(m.value, back)
        rebalance()
        back
    member.polledAt = now
    val owned = ownedBy(m.value)
    val revoked = member.held.keysIterator.filterNot(p => owned.exists(_ == p) && member.held(p) == ownership(p.index)).toVector.sortBy(_.index)
    revoked.foreach(member.held.remove)
    val gained = owned.filterNot(member.held.contains)
    gained.foreach: p =>
      member.held(p) = ownership(p.index)
      member.position(p) = committedOffsets.getOrElse(p, Offset(0))
    val rebalances = revoked.map(Delivery.Revoked(_)) ++ gained.map(p => Delivery.Assigned(p, member.position(p)))
    val reads = owned.flatMap: p =>
      val from = member.position(p).value.toInt
      val read = logs(p.index).view.drop(from).take(maxRecords).toVector
      member.position(p) = Offset((from + read.size).toLong)
      read
    val ends = owned.filter(p => member.position(p) == endOffset(p)).map(Delivery.ReachedEnd(_))
    rebalances ++ reads ++ ends

  /** Taken only under the assignment the member knows of, which a change of owner replaces: a zombie's commit is
    * rejected once a rebalance has moved the partition away (README section 5).
    */
  def commit(m: MemberId, c: Commit): Boolean =
    val current = members.get(m.value).exists(_.held.get(c.partition).contains(ownership(c.partition.index)))
    if current then committedOffsets(c.partition) = c.next
    current

  private def ownedBy(id: Long): Vector[Partition] =
    (0 until partitions).toVector.map(Partition(_)).filter(p => owners.get(p).contains(id))

  /** Round robin over the members in join order. A partition that changes owner gets a new ownership number. */
  private def rebalance(): Unit =
    val ids = members.keysIterator.toVector
    for i <- 0 until partitions do
      val p = Partition(i)
      val next = if ids.isEmpty then None else Some(ids(i % ids.size))
      if owners.get(p) != next then
        ownership(i) += 1
        next match
          case Some(id) => owners(p) = id
          case None => owners.remove(p)

object SimKafka:
  /** A member of the group. An opaque `Long`: stored as a primitive. */
  opaque type MemberId = Long

  object MemberId:
    def apply(value: Long): MemberId = value
    extension (m: MemberId) def value: Long = m
    given CanEqual[MemberId, MemberId] = CanEqual.derived

  private final class Member(var polledAt: Long):
    val held = mutable.HashMap.empty[Partition, Long]
    val position = mutable.HashMap.empty[Partition, Offset]
