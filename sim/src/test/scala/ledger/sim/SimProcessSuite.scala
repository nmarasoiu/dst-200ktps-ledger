package ledger.sim

import ledger.core.*
import ledger.core.Fixtures.*
import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll
import scala.concurrent.duration.*

class SimProcessSuite extends ScalaCheckSuite:

  private def t(ms: Long) = HistoryTime(ms)
  private val clock = SkewedClock[Clock.Edge](1000)

  private def pinger(marks: MarkCounts = new MarkCounts) = new SimProcess(Pingers.machine, clock.read, marks)

  property("a process's clock reads the history's time plus its skew, and a wake-up comes back to the history's time"):
    forAll: (skew: Int, ms: Int) =>
      val c = SkewedClock[Clock.Edge](skew.toLong)
      c.read(t(ms.toLong)).millis == ms.toLong + skew && c.toHistory(c.read(t(ms.toLong))) == t(ms.toLong)

  test("each step reads the process's own clock, and its marks are counted"):
    val marks = new MarkCounts
    val p = pinger(marks)
    assertEquals(
      p.deliver(p.incarnation, t(50), Pingers.Event.Tick),
      Vector(Pingers.Effect.Ping(0), Pingers.Effect.WakeAt(EdgeTime(1050 + Pingers.TickEvery.toMillis))),
    )
    assertEquals(marks(Mark.EdgeAnswered), 1L)

  test("a crash drops the state, and a process that is down drops what comes"):
    val p = pinger()
    p.deliver(p.incarnation, t(0), Pingers.Event.Tick)
    p.crash()
    assertEquals(p.status, SimProcess.Life.Down)
    assertEquals(p.state, Pingers.machine.initial)
    assertEquals(p.deliver(p.incarnation, t(1), Pingers.Event.Tick), Vector.empty)
    assertEquals(p.state, Pingers.machine.initial)

  test("a new life never hears a reply or a wake-up meant for an earlier one"):
    val p = pinger()
    val first = p.incarnation
    p.deliver(first, t(0), Pingers.Event.Tick)
    p.crash()
    p.restart()
    assert(p.incarnation != first)
    assertEquals(p.deliver(first, t(1), Pingers.Event.Pong(0)), Vector.empty)
    assertEquals(p.state, Pingers.machine.initial)
    p.deliver(p.incarnation, t(2), Pingers.Event.Tick)
    assertEquals(p.state.sent, 1L)

  test("a paused process holds its events, then runs them in order by its clock when it resumes"):
    val p = pinger()
    p.pause()
    assertEquals(p.deliver(p.incarnation, t(10), Pingers.Event.Tick), Vector.empty)
    assertEquals(p.deliver(p.incarnation, t(20), Pingers.Event.Tick), Vector.empty)
    assertEquals(p.state, Pingers.machine.initial)
    val wake = EdgeTime(1500 + Pingers.TickEvery.toMillis)
    assertEquals(
      p.resume(t(500)),
      Vector(Pingers.Effect.Ping(0), Pingers.Effect.WakeAt(wake), Pingers.Effect.Ping(1), Pingers.Effect.WakeAt(wake)),
    )
    assertEquals(p.state.sent, 2L)

  test("a crash during a pause loses what it held"):
    val p = pinger()
    p.pause()
    p.deliver(p.incarnation, t(10), Pingers.Event.Tick)
    p.crash()
    p.restart()
    assertEquals(p.state, Pingers.machine.initial)
    p.pause()
    assertEquals(p.resume(t(20)), Vector.empty)

  test("a fault that does not fit the process's life is a bug in the simulator"):
    val p = pinger()
    intercept[IllegalArgumentException](p.restart())
    intercept[IllegalArgumentException](p.resume(t(0)))
    p.crash()
    intercept[IllegalArgumentException](p.crash())
    intercept[IllegalArgumentException](p.pause())

  // The real machines fit the same shape (README section 11, processes).

  test("the edge runs as a process: a crash drops its requests, and a reply to the earlier life is dropped"):
    val cfg = Edge.Config(EdgeCfg, Admission.Config(maxInFlight = 2, maxLedgerLatency = 200.millis, partitions = 8))
    val (monotonic, wall) = (SkewedClock[Clock.Edge](1000), SkewedClock[Clock.Wall](T0))
    val p = new SimProcess(Edge.machine(cfg), at => Edge.Now(monotonic.read(at), wall.read(at)), new MarkCounts)
    val k = key(1)
    val first = p.incarnation
    assert(p.deliver(first, t(0), Edge.Event.Arrived(Connection(1), authorize(k))).exists(_.isInstanceOf[Edge.Effect.Append]))
    assertEquals(p.state.requests.size, 1)
    p.crash()
    p.restart()
    assertEquals(p.state, Edge.State.Initial)
    val stale = Edge.Event.Written(Edge.Ref.Caller(Connection(1)), Record.CommandRecord(authCommand(k)), CommandLog.Written.Acknowledged)
    assertEquals(p.deliver(first, t(10), stale), Vector.empty)
    assertEquals(p.state, Edge.State.Initial)

  test("a paused resolver wakes by its clock now: the command it read before the pause is due, and it fences it"):
    import CommandLog.Delivery
    import Resolver.Event.Delivered
    val partition = Partition(0)
    val auth = authCommand(key(1))
    val marks = new MarkCounts
    val p = new SimProcess(Resolver.machine(ResolverCfg), SkewedClock[Clock.Resolver](0).read, marks)
    p.deliver(p.incarnation, t(0), Delivered(Delivery.Assigned(partition, Offset(0))))
    p.deliver(p.incarnation, t(0), Delivered(Delivery.Read(partition, Offset(0), LogTime(0), Record.CommandRecord(auth))))
    p.pause()
    assertEquals(p.deliver(p.incarnation, t(1), Delivered(Delivery.ReachedEnd(partition))), Vector.empty)
    val fence = Resolver.Effect.Submit(Resolver.Ref(partition, auth.id, Generation(1)), Ledger.Submit(Period(0), Vector(Transfers.fence(auth.a, LedgerCfg))))
    assertEquals(p.resume(t(Due)), Vector[Resolver.Effect](fence))
    assertEquals(marks(Mark.ResolverJudgedIdlePartition), 1L)
