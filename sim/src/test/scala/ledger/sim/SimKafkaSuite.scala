package ledger.sim

import ledger.core.*
import ledger.core.CommandLog.{Commit, Delivery}

/** What the contract suites cannot pin on a real broker, whose timing they do not control. */
class SimKafkaSuite extends munit.FunSuite:
  private val Timeout = 1000L
  private val P0 = Partition(0)

  private def record(n: Int): Record = Record.CommandRecord(Command.Cancel(ClientKey(WallTime(1000L + n), s"k$n")))

  private def kafka(partitions: Int = 1) = new SimKafka(partitions, Timeout)

  test("a member that left and came back holds the partition under a new assignment, so its stale commit is rejected"):
    val k = kafka()
    val zombie = k.join(0L)
    k.poll(zombie, 0L)
    k.expireMembers(Timeout)
    val other = k.join(Timeout)
    k.poll(other, Timeout)
    // The zombie joins again and the partition comes back to it, but it has not heard so yet.
    k.leave(other)
    k.expireMembers(Timeout)
    assertEquals(k.commit(zombie, Commit(P0, Offset(1))), false)
    assertEquals(k.committed(P0), None)

  test("a partition with a new owner restarts from the commit, not from the old owner's position"):
    val k = kafka()
    (1 to 3).foreach(n => k.land(record(n), LogTime(n.toLong)))
    val first = k.join(0L)
    assertEquals(k.poll(first, 0L).count(_.isInstanceOf[Delivery.Read]), 3)
    assert(k.commit(first, Commit(P0, Offset(1))))
    k.leave(first)
    val second = k.join(0L)
    val told = k.poll(second, 0L)
    assert(told.contains(Delivery.Assigned(P0, Offset(1))))
    assertEquals(told.collect { case r: Delivery.Read => r.offset }, Vector(Offset(1), Offset(2)))

  test("a poll takes at most its limit from a partition, and tells the end only past the last record"):
    val k = kafka()
    (1 to 3).foreach(n => k.land(record(n), LogTime(n.toLong)))
    val m = k.join(0L)
    val first = k.poll(m, 0L, maxRecords = 2)
    assertEquals(first.collect { case r: Delivery.Read => r.offset }, Vector(Offset(0), Offset(1)))
    assert(!first.contains(Delivery.ReachedEnd(P0)))
    val second = k.poll(m, 0L, maxRecords = 2)
    assertEquals(second.collect { case r: Delivery.Read => r.offset }, Vector(Offset(2)))
    assert(second.contains(Delivery.ReachedEnd(P0)))

  test("a landed record shows to readers whatever its writer heard, and append times are whatever the leader stamped"):
    val k = kafka()
    k.setLeader(P0, present = false)
    k.land(record(1), LogTime(50L))
    k.setLeader(P0, present = true)
    k.land(record(2), LogTime(40L))
    assertEquals(k.records(P0).map(_.appended.millis), Vector(50L, 40L))
    assertEquals(k.endOffset(P0), Offset(2))

  test("partitions are assigned round robin in join order, the same on every run"):
    def owners(): Vector[Vector[Delivery]] =
      val k = kafka(5)
      val (a, b) = (k.join(0L), k.join(0L))
      Vector(k.poll(a, 0L), k.poll(b, 0L))
    assertEquals(owners(), owners())
    assertEquals(owners()(0).collect { case Delivery.Assigned(p, _) => p.index }, Vector(0, 2, 4))
    assertEquals(owners()(1).collect { case Delivery.Assigned(p, _) => p.index }, Vector(1, 3))

  test("a member is dropped only after the timeout, and a poll keeps it in"):
    val k = kafka()
    val (a, b) = (k.join(0L), k.join(0L))
    k.poll(a, Timeout - 1)
    k.expireMembers(Timeout - 1)
    assert(k.poll(b, Timeout - 1).isEmpty)
    k.poll(a, Timeout + 5)
    k.expireMembers(Timeout)
    assertEquals(k.poll(a, Timeout + 5).count(_.isInstanceOf[Delivery.Assigned]), 0)

  test("a member that has not heard of a rebalance can commit what it kept, and not what moved"):
    val k = kafka(2)
    val a = k.join(0L)
    k.poll(a, 0L)
    val b = k.join(0L)
    assertEquals(k.commit(a, Commit(Partition(1), Offset(1))), false)
    assertEquals(k.commit(a, Commit(P0, Offset(1))), true)
    assertEquals(k.committed(Partition(1)), None)
    k.poll(b, 0L)
    assertEquals(k.commit(b, Commit(Partition(1), Offset(1))), true)
