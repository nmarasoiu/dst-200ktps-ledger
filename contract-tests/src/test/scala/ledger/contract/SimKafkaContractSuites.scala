package ledger.contract

import cats.effect.{IO, Resource}
import ledger.core.*
import ledger.core.CommandLog.{Commit, Delivery, Leaders, Written}
import ledger.sim.SimKafka
import scala.concurrent.duration.*

/** The simulated log's clock, shared by every topic of a suite. Time passes only when a suite says so. Several fibers
  * call at once, as on the real clients, and the simulator is not made for threads, hence the lock.
  */
private final class SimLogWorld:
  private var millis = 1_700_000_000_000L
  private var topics = Vector.empty[SimKafka]

  def atomically[A](body: => A): IO[A] = IO(synchronized(body))
  def clock: Long = millis

  def newTopic(partitions: Int): SimKafka =
    // The same limit as the real suite's `max.poll.interval.ms`.
    val made = new SimKafka(partitions, 1500L)
    topics :+= made
    made

  def passTime(d: FiniteDuration): IO[Unit] = atomically:
    millis += d.toMillis
    topics.foreach(_.expireMembers(millis))

class SimKafkaContractSuite extends LogContract:
  private val world = new SimLogWorld
  import world.atomically

  val log: LogUnderTest = new LogUnderTest:
    def passTime(d: FiniteDuration): IO[Unit] = world.passTime(d)
    def now: IO[Long] = atomically(world.clock)
    def topic(count: Int): IO[TopicUnderTest] = atomically:
      val kafka = world.newTopic(count)
      new TopicUnderTest:
        def partitions: Int = count
        def append(record: Record): IO[Written] = atomically(kafka.append(record, LogTime(world.clock)))
        def endOffset(partition: Partition): IO[Offset] = atomically(kafka.endOffset(partition))
        def committed(partition: Partition): IO[Option[Offset]] = atomically(kafka.committed(partition))
        def member: Resource[IO, MemberUnderTest] =
          Resource.make(atomically(kafka.join(world.clock)))(m => atomically(kafka.leave(m))).map: m =>
            new MemberUnderTest:
              def poll: IO[Vector[Delivery]] = atomically(kafka.poll(m, world.clock))
              def commit(commit: Commit): IO[Boolean] = atomically(kafka.commit(m, commit))

class SimKafkaFaultsContractSuite extends LogFaultsContract:
  private val world = new SimLogWorld
  import world.atomically

  private val P0 = Partition(0)

  private def faulty(strictly: Boolean): IO[FaultyTopicUnderTest] = atomically:
    val kafka = world.newTopic(1)
    new FaultyTopicUnderTest:
      def append(record: Record): IO[Written] = atomically(kafka.append(record, LogTime(world.clock)))
      def endOffset: IO[Offset] = atomically(kafka.endOffset(P0))
      def readAll: IO[Vector[Delivery.Read]] = atomically(kafka.records(P0))
      def leaders: IO[Leaders] = atomically(kafka.leaders)
      def withLeaderFailed[A](body: IO[A]): IO[A] = body
      def withoutLeader[A](body: IO[A]): IO[A] =
        atomically(kafka.setLeader(P0, present = false)) >> body <* atomically(kafka.setLeader(P0, present = true))
      def withReplicaOutOfSync[A](body: IO[A]): IO[A] =
        atomically(kafka.setTooFewInSync(P0, strictly)) >> body <* atomically(kafka.setTooFewInSync(P0, false))
      def appendTimingOutThenLanding(record: Record): IO[Written] = atomically:
        kafka.land(record, LogTime(world.clock))
        Written.Failed
      def appendLostWithItsLeader(record: Record): IO[Written] = IO.pure(Written.Failed)

  val log: FaultyLogUnderTest = new FaultyLogUnderTest:
    def replicated: IO[FaultyTopicUnderTest] = faulty(strictly = false)
    def unreplicated: IO[FaultyTopicUnderTest] = faulty(strictly = false)
    def strict: IO[FaultyTopicUnderTest] = faulty(strictly = true)
    def passTime(d: FiniteDuration): IO[Unit] = world.passTime(d)
