package ledger.contract

import cats.effect.{IO, Resource}
import ledger.core.*
import scala.concurrent.duration.FiniteDuration

/** The command log port as the suites see it: a real broker through the Kafka clients, or the simulated one. The
  * shell's fs2-kafka adapter comes in stage 5; these suites hold what it stands on (README section 14, stage 4).
  */
trait LogUnderTest:
  /** A new topic with these partitions, for one test. */
  def topic(partitions: Int): IO[TopicUnderTest]

  /** Lets this much time pass: a real wait, or the simulated clock moving on. */
  def passTime(d: FiniteDuration): IO[Unit]

  /** The clock append times are compared with, in epoch milliseconds. */
  def now: IO[Long]

trait TopicUnderTest:
  def partitions: Int

  /** Appends a record to the partition of its key, and waits for the in-sync replicas. */
  def append(record: Record): IO[CommandLog.Written]

  def endOffset(partition: Partition): IO[Offset]

  /** The group's committed offset, by the coordinator. */
  def committed(partition: Partition): IO[Option[Offset]]

  /** A new member of the topic's one consumer group. Releasing it leaves the group. */
  def member: Resource[IO, MemberUnderTest]

trait MemberUnderTest:
  /** One poll: rebalances, records, and the end of each partition it has reached. */
  def poll: IO[Vector[CommandLog.Delivery]]

  /** Whether the coordinator took the commit. */
  def commit(commit: CommandLog.Commit): IO[Boolean]

/** Faults that need more than one broker, each named by what the design relies on. A real cluster gets there by
  * pausing brokers, and the simulated one by its own controls.
  */
trait FaultyLogUnderTest:
  /** A new topic of one partition, on three replicas of which two must be in sync. */
  def replicated: IO[FaultyTopicUnderTest]

  /** A new topic of one partition with one replica, so losing its leader leaves it without one. */
  def unreplicated: IO[FaultyTopicUnderTest]

  /** A new topic of one partition on three replicas, all of which must be in sync. */
  def strict: IO[FaultyTopicUnderTest]

  def passTime(d: FiniteDuration): IO[Unit]

trait FaultyTopicUnderTest:
  def append(record: Record): IO[CommandLog.Written]

  def endOffset: IO[Offset]

  /** Every record of the partition, read back without a group. */
  def readAll: IO[Vector[CommandLog.Delivery.Read]]

  /** The partitions without a leader in the producer's metadata. */
  def leaders: IO[CommandLog.Leaders]

  /** Runs `body` with the leader down and another replica leading in its place, then brings the old leader back. */
  def withLeaderFailed[A](body: IO[A]): IO[A]

  /** Runs `body` while the partition has no leader, then gives it one again. */
  def withoutLeader[A](body: IO[A]): IO[A]

  /** Runs `body` while a replica is out of sync, then brings it back. */
  def withReplicaOutOfSync[A](body: IO[A]): IO[A]

  /** Appends so the send times out while a replica holds up the acknowledgement. The record lands afterwards. */
  def appendTimingOutThenLanding(record: Record): IO[CommandLog.Written]

  /** Appends so the send times out, and the leader that holds it fails before any other replica has it. */
  def appendLostWithItsLeader(record: Record): IO[CommandLog.Written]
