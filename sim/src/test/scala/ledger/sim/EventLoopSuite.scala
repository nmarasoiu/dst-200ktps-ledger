package ledger.sim

import ledger.core.*
import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll
import scala.concurrent.duration.*

class EventLoopSuite extends ScalaCheckSuite:

  private def t(ms: Long) = HistoryTime(ms)

  private def drain(loop: EventLoop[String], until: Long): Vector[(Long, String)] =
    var seen = Vector.empty[(Long, String)]
    loop.run(t(until))(e => seen :+= (loop.now.millis, e))
    seen

  test("events run in time order, and at one time in the order they were scheduled"):
    val loop = new EventLoop[String]
    loop.schedule(t(30), "c")
    loop.schedule(t(10), "a1")
    loop.schedule(t(10), "a2")
    loop.schedule(t(20), "b")
    assertEquals(drain(loop, 100), Vector(10L -> "a1", 10L -> "a2", 20L -> "b", 30L -> "c"))

  test("a handler can schedule more events, and a delay counts from now"):
    val loop = new EventLoop[String]
    var seen = Vector.empty[(Long, String)]
    loop.schedule(t(10), "first")
    loop.run(t(100)): e =>
      seen :+= (loop.now.millis, e)
      if e == "first" then loop.after(5.millis, "second")
    assertEquals(seen, Vector(10L -> "first", 15L -> "second"))

  test("a time already past means as soon as possible"):
    val loop = new EventLoop[String]
    var seen = Vector.empty[(Long, String)]
    loop.schedule(t(50), "first")
    loop.run(t(100)): e =>
      seen :+= (loop.now.millis, e)
      if e == "first" then loop.schedule(t(10), "late")
    assertEquals(seen, Vector(50L -> "first", 50L -> "late"))

  test("a run stops at its end, leaves later events queued and moves the clock there"):
    val loop = new EventLoop[String]
    loop.schedule(t(10), "soon")
    loop.schedule(t(100), "at the end")
    loop.schedule(t(101), "later")
    assertEquals(drain(loop, 100), Vector(10L -> "soon", 100L -> "at the end"))
    assertEquals(loop.now, t(100))
    assertEquals(drain(loop, 200), Vector(101L -> "later"))
    assertEquals(loop.now, t(200))

  property("the clock never goes back, whatever is scheduled"):
    forAll: (times: List[Short]) =>
      val loop = new EventLoop[String]
      times.foreach(ms => loop.schedule(t(ms.toLong.abs), "e"))
      val seen = drain(loop, Short.MaxValue.toLong + 1).map(_._1)
      seen == seen.sorted && seen.size == times.size
