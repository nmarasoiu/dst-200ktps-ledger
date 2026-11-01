package ledger.sim

import ledger.core.*
import scala.concurrent.duration.*

/** A small pipeline of its own, to test the simulator's parts together before its real adapters exist. Each pinger
  * ticks on its own skewed clock and sends a ping; the network returns a pong after a delay the seed chooses. A pong
  * can only answer a ping of the same life, so a pinger with more pongs than pings heard from an earlier life.
  */
object Pingers:
  final case class State(sent: Long, pongs: Long) derives CanEqual

  enum Event derives CanEqual:
    case Tick
    case Pong(n: Long)

  enum Effect derives CanEqual:
    case Ping(n: Long)
    case WakeAt(at: EdgeTime)

  val TickEvery: FiniteDuration = 100.millis

  val machine: Machine[State, Event, Effect, EdgeTime] = Machine(
    State(0, 0),
    (s, now, e) =>
      e match
        case Event.Tick => Step(s.copy(sent = s.sent + 1), Vector(Effect.Ping(s.sent), Effect.WakeAt(now + TickEvery)), Vector(Mark.EdgeAnswered))
        case Event.Pong(_) => Step(s.copy(pongs = s.pongs + 1), marks = Vector(Mark.ResolverFoundOutcome)),
  )

  enum SimEvent derives CanEqual:
    case Deliver(target: Int, to: Incarnation, event: Event)
    case Inject(fault: Fault[Int])

  val StopAt: HistoryTime = HistoryTime(5000)
  val EndAt: HistoryTime = HistoryTime(7000)

  val Faults: FaultPlan.Config = FaultPlan.Config(StopAt, maxGap = 200.millis, maxOutage = 800.millis, crashChance = 0.5)

  final case class Run(
      result: RunResult,
      plan: Vector[TimedFault[Int]],
      lives: Vector[SimProcess.Life],
      ticksAfterStop: Vector[Long],
      trace: Vector[String],
  )

  /** `failOn` seeds report a violation, as a planted bug would. */
  def run(seed: Long, pingers: Int = 3, failOn: Set[Long] = Set.empty, keepTrace: Boolean = false): Run =
    val rng = new Rng(seed)
    val loop = new EventLoop[SimEvent]
    val marks = new MarkCounts
    val trace = new Trace(keepTrace)
    val clocks = Vector.fill(pingers)(SkewedClock.chosen[Clock.Edge](rng, 2.seconds))
    val processes = clocks.map(c => new SimProcess(machine, c.read, marks))
    val plan = FaultPlan.generate(rng, processes.indices.toVector, Faults)
    val ticksAfterStop = new Array[Long](pingers)
    var violations = Vector.empty[Violation]

    def perform(i: Int, effects: Vector[Effect]): Unit =
      val life = processes(i).incarnation
      effects.foreach:
        case Effect.Ping(n) => loop.after(rng.between(1, 300).millis, SimEvent.Deliver(i, life, Event.Pong(n)))
        case Effect.WakeAt(at) => loop.schedule(clocks(i).toHistory(at), SimEvent.Deliver(i, life, Event.Tick))

    def tick(i: Int): Unit = loop.schedule(loop.now, SimEvent.Deliver(i, processes(i).incarnation, Event.Tick))

    plan.foreach(f => loop.schedule(f.at, SimEvent.Inject(f.fault)))
    processes.indices.foreach(tick)
    loop.run(EndAt): event =>
      trace.record(loop.now, event)
      event match
        case SimEvent.Deliver(i, to, e) =>
          if e == Event.Tick && loop.now >= StopAt && to == processes(i).incarnation then ticksAfterStop(i) += 1
          perform(i, processes(i).deliver(to, loop.now, e))
          val s = processes(i).state
          if s.pongs > s.sent then violations :+= Violation("A pong answers a ping of the same life", s"pinger $i: $s")
        case SimEvent.Inject(Fault.Crash(i)) => processes(i).crash()
        case SimEvent.Inject(Fault.Restart(i)) =>
          processes(i).restart()
          tick(i)
        case SimEvent.Inject(Fault.Pause(i)) => processes(i).pause()
        case SimEvent.Inject(Fault.Resume(i)) => perform(i, processes(i).resume(loop.now))
    if failOn(seed) then violations :+= Violation("planted", s"seed $seed")
    Run(RunResult(violations, marks, trace.fingerprint), plan, processes.map(_.status), ticksAfterStop.toVector, trace.lines)
