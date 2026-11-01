package ledger.contract

import cats.effect.IO
import cats.syntax.all.*
import java.util.concurrent.atomic.AtomicLong
import ledger.core.*
import ledger.core.CommandLog.{Commit, Delivery, Written}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Partitions, offsets, groups, append times and redelivery (README sections 4, 5 and 6), held against a real broker
  * and the simulated one alike. What needs more than one broker is `LogFaultsContract`.
  */
abstract class LogContract extends CatsEffectSuite:

  def log: LogUnderTest

  override def munitIOTimeout: Duration = 2.minutes

  private val counter = new AtomicLong(0)

  private def record(): Record =
    val n = counter.incrementAndGet()
    Record.CommandRecord(Command.Cancel(ClientKey(WallTime(1_700_000_000_000L + n), s"log-$n")))

  /** `count` new records for one partition, whichever keys land there. */
  private def recordsFor(partition: Partition, partitions: Int, count: Int): Vector[Record] =
    Iterator.continually(record()).filter(r => Partitioner.partition(r.command.a, partitions) == partition).take(count).toVector

  private def appendAll(topic: TopicUnderTest, records: Vector[Record]): IO[Unit] =
    records.traverse_(r => topic.append(r).assertEquals(Written.Acknowledged))

  /** Polls until what has been told satisfies `done`, and returns all of it. */
  private def pollUntil(member: MemberUnderTest)(done: Vector[Delivery] => Boolean): IO[Vector[Delivery]] =
    def go(told: Vector[Delivery], left: Int): IO[Vector[Delivery]] =
      if done(told) then IO.pure(told)
      else if left == 0 then IO(fail(s"never got there; told $told"))
      else member.poll.flatMap(more => log.passTime(50.millis) >> go(told ++ more, left - 1))
    go(Vector.empty, 400)

  private def assignedTo(told: Vector[Delivery]): Set[Partition] =
    told.foldLeft(Set.empty[Partition]):
      case (owned, Delivery.Assigned(p, _)) => owned + p
      case (owned, Delivery.Revoked(p)) => owned - p
      case (owned, _) => owned

  private def reads(told: Vector[Delivery]): Vector[Delivery.Read] = told.collect { case r: Delivery.Read => r }

  private def reachedEnd(told: Vector[Delivery], p: Partition): Boolean = told.exists(_ == Delivery.ReachedEnd(p))

  private val P0 = Partition(0)

  test("a key's records land on the partition the core computes, Kafka's default partitioner"):
    val partitions = 8
    val records = Vector.fill(64)(record())
    for
      topic <- log.topic(partitions)
      _ <- appendAll(topic, records)
      read <- topic.member.use: m =>
        pollUntil(m)(told => reads(told).size == records.size).map(reads)
      _ = read.foreach(r => assertEquals(r.partition, Partitioner.partition(r.record.command.a, partitions), r))
      _ = assertEquals(read.map(_.record).toSet, records.toSet)
    yield ()

  test("offsets count from 0 in append order, within a partition"):
    val records = recordsFor(P0, 2, 5)
    for
      topic <- log.topic(2)
      _ <- appendAll(topic, records)
      read <- topic.member.use(m => pollUntil(m)(told => reads(told).size == 5).map(reads))
      _ = assertEquals(read.map(_.offset), (0L until 5L).toVector.map(Offset(_)))
      _ = assertEquals(read.map(_.record), records)
    yield ()

  test("the append time is the leader's clock, not the producer's, and does not go back in a run"):
    val records = recordsFor(P0, 1, 5)
    for
      topic <- log.topic(1)
      before <- log.now
      _ <- appendAll(topic, records)
      after <- log.now
      read <- topic.member.use(m => pollUntil(m)(told => reads(told).size == 5).map(reads))
      times = read.map(_.appended.millis)
      _ = assert(times.forall(t => t >= before - 5000 && t <= after + 5000), s"$times outside $before to $after")
      _ = assertEquals(times, times.sorted)
    yield ()

  test("the end offset is one past the last acknowledged write, and a member hears when it reaches it"):
    val records = recordsFor(P0, 1, 3)
    for
      topic <- log.topic(1)
      _ <- topic.endOffset(P0).assertEquals(Offset(0))
      _ <- appendAll(topic, records)
      _ <- topic.endOffset(P0).assertEquals(Offset(3))
      _ <- topic.member.use: m =>
        for
          told <- pollUntil(m)(told => reads(told).size == 3 && reachedEnd(told, P0))
          lastRead = told.lastIndexWhere(_.isInstanceOf[Delivery.Read])
          // The end is told only past the last record, never before it.
          _ = assert(told.indexOf(Delivery.ReachedEnd(P0)) > lastRead, told)
          _ <- appendAll(topic, recordsFor(P0, 1, 1))
          more <- pollUntil(m)(told => reads(told).size == 1 && reachedEnd(told, P0))
          _ = assertEquals(reads(more).map(_.offset), Vector(Offset(3)))
        yield ()
    yield ()

  test("an idle partition keeps telling its end to a member that keeps polling"):
    for
      topic <- log.topic(1)
      _ <- topic.member.use: m =>
        for
          _ <- pollUntil(m)(reachedEnd(_, P0))
          _ <- pollUntil(m)(reachedEnd(_, P0))
        yield ()
    yield ()

  test("a member with no commit behind it is assigned every partition from 0"):
    for
      topic <- log.topic(3)
      told <- topic.member.use(m => pollUntil(m)(assignedTo(_).size == 3))
      _ = assertEquals(
        told.collect { case Delivery.Assigned(p, from) => (p, from) }.toSet,
        (0 until 3).map(i => (Partition(i), Offset(0))).toSet,
      )
    yield ()

  test("a commit is read back, and the distance from it to the end is the lag"):
    val records = recordsFor(P0, 1, 5)
    for
      topic <- log.topic(1)
      _ <- appendAll(topic, records)
      _ <- topic.member.use: m =>
        for
          _ <- pollUntil(m)(told => reads(told).size == 5)
          _ <- topic.committed(P0).assertEquals(None)
          _ <- m.commit(Commit(P0, Offset(3))).assertEquals(true)
          _ <- topic.committed(P0).assertEquals(Some(Offset(3)))
          end <- topic.endOffset(P0)
          _ = assertEquals(end.value - 3L, 2L)
        yield ()
    yield ()

  test("a member after a restart is redelivered everything from the committed offset, and nothing before it"):
    val records = recordsFor(P0, 1, 5)
    for
      topic <- log.topic(1)
      _ <- appendAll(topic, records)
      _ <- topic.member.use: m =>
        pollUntil(m)(told => reads(told).size == 5) >> m.commit(Commit(P0, Offset(3))).assertEquals(true)
      told <- topic.member.use(m => pollUntil(m)(told => reads(told).size == 2 && reachedEnd(told, P0)))
      _ = assert(told.contains(Delivery.Assigned(P0, Offset(3))), told)
      _ = assertEquals(reads(told).map(_.offset), Vector(Offset(3), Offset(4)))
      _ = assertEquals(reads(told).map(_.record), records.drop(3))
    yield ()

  test("a record never committed is redelivered, however many times"):
    val records = recordsFor(P0, 1, 2)
    for
      topic <- log.topic(1)
      _ <- appendAll(topic, records)
      _ <- topic.member.use(m => pollUntil(m)(told => reads(told).size == 2))
      again <- topic.member.use(m => pollUntil(m)(told => reads(told).size == 2).map(reads))
      _ = assertEquals(again.map(_.record), records)
    yield ()

  test("a second member takes a share, each partition has one owner, and a moved partition restarts from its commit"):
    val partitions = 4
    val records = (0 until partitions).toVector.flatMap(i => recordsFor(Partition(i), partitions, 2))
    for
      topic <- log.topic(partitions)
      _ <- appendAll(topic, records)
      _ <- topic.member.use: first =>
        for
          told <- pollUntil(first)(told => reads(told).size == records.size)
          _ = assertEquals(assignedTo(told).size, partitions)
          _ <- (0 until partitions).toVector.traverse_(i => first.commit(Commit(Partition(i), Offset(1))).assertEquals(true))
          _ <- topic.member.use: second =>
            def settle(a: Vector[Delivery], b: Vector[Delivery], left: Int): IO[(Vector[Delivery], Vector[Delivery])] =
              val (x, y) = (assignedTo(told ++ a), assignedTo(b))
              if x.nonEmpty && y.nonEmpty && (x ++ y).size == partitions && x.intersect(y).isEmpty then IO.pure((a, b))
              else if left == 0 then IO(fail(s"never settled: $x $y"))
              else (first.poll, second.poll).parTupled.flatMap((more, others) => settle(a ++ more, b ++ others, left - 1))
            for
              (a, b) <- settle(Vector.empty, Vector.empty, 200)
              moved = assignedTo(b)
              _ = assert(a.exists(_.isInstanceOf[Delivery.Revoked]), a)
              _ = assert(b.collect { case Delivery.Assigned(_, from) => from }.forall(_ == Offset(1)), b)
              // What the new owner reads again, from each partition it took.
              rest <- pollUntil(second)(told => reads(b ++ told).size >= moved.size)
              redelivered = reads(b ++ rest)
              _ = assertEquals(redelivered.map(_.partition).toSet, moved)
              _ = assert(redelivered.forall(_.offset == Offset(1)), redelivered)
            yield ()
        yield ()
    yield ()

  test("a member that leaves gives its partitions back"):
    val partitions = 2
    for
      topic <- log.topic(partitions)
      _ <- topic.member.use: first =>
        for
          told <- pollUntil(first)(assignedTo(_).size == partitions)
          settled <- topic.member.use: second =>
            def settle(a: Vector[Delivery], left: Int): IO[Vector[Delivery]] =
              if assignedTo(told ++ a).size == 1 then IO.pure(a)
              else if left == 0 then IO(fail("never settled"))
              else (first.poll, second.poll).parTupled.flatMap((more, _) => settle(a ++ more, left - 1))
            settle(Vector.empty, 200)
          later <- pollUntil(first)(more => assignedTo(told ++ settled ++ more).size == partitions)
          _ = assertEquals(assignedTo(told ++ settled ++ later).size, partitions)
        yield ()
    yield ()

  // A zombie resolver: README section 5.

  test("a member that goes quiet loses its partitions, and its commit is rejected once they have moved"):
    val records = recordsFor(P0, 1, 4)
    for
      topic <- log.topic(1)
      _ <- appendAll(topic, records)
      _ <- topic.member.use: zombie =>
        for
          _ <- pollUntil(zombie)(told => reads(told).size == 4)
          _ <- zombie.commit(Commit(P0, Offset(1))).assertEquals(true)
          _ <- topic.member.use: successor =>
            for
              // The zombie stops polling, and the successor keeps going until it owns the partition.
              told <- pollUntil(successor)(told => assignedTo(told).contains(P0))
              _ = assert(told.contains(Delivery.Assigned(P0, Offset(1))), told)
              _ <- successor.commit(Commit(P0, Offset(2))).assertEquals(true)
              _ <- zombie.commit(Commit(P0, Offset(4))).assertEquals(false)
              _ <- topic.committed(P0).assertEquals(Some(Offset(2)))
              // Its writes still land: only its commit is fenced.
              _ <- appendAll(topic, recordsFor(P0, 1, 1))
              _ <- pollUntil(successor)(told => reads(told).exists(_.offset == Offset(4)))
            yield ()
        yield ()
    yield ()
