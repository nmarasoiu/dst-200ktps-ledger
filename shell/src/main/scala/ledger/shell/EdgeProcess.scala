package ledger.shell

import cats.effect.std.{Queue, Supervisor}
import cats.effect.{Deferred, IO, Ref, Resource}
import ledger.core.*
import scala.concurrent.duration.*

/** An edge instance: it runs the edge's machine and performs its effects (README section 11, processes). All it holds
  * is the requests in flight and who is waiting on each, so a crash leaves nothing to restore (README section 8,
  * failover). Each effect runs on its own fiber and comes back as an event, so a slow ledger holds up no other
  * request (README section 10, waiting without blocking).
  */
trait EdgeProcess:
  /** A request from a caller, answered by the machine. A caller that gives up leaves the request running: it is the
    * log and the resolver that finish it.
    */
  def ask(request: Request): IO[Reply]

object EdgeProcess:

  def resource(
      cfg: Edge.Config,
      log: LogWriter,
      ledger: LedgerCalls,
      alarm: Alarm => IO[Unit],
      leadersEvery: FiniteDuration = 200.millis,
  ): Resource[IO, EdgeProcess] =
    for
      supervisor <- Supervisor[IO]
      events <- Resource.eval(Queue.bounded[IO, Edge.Event](MachineLoop.EventCapacity))
      waiting <- Resource.eval(Ref[IO].of(Map.empty[Connection, Deferred[IO, Reply]]))
      connections <- Resource.eval(Ref[IO].of(0L))
      now = (IO.monotonic, IO.realTime).mapN2((m, w) => Edge.Now(EdgeTime(m.toMillis), WallTime(w.toMillis)))
      perform = (f: Edge.Effect) =>
        f match
          case Edge.Effect.Answer(connection, reply) =>
            waiting.modify(all => (all - connection, all.get(connection))).flatMap(_.fold(IO.unit)(_.complete(reply).void))
          case Edge.Effect.Append(ref, append) =>
            supervisor.supervise(log.append(append).flatMap(written => events.offer(Edge.Event.Written(ref, append.record, written)))).void
          case Edge.Effect.Submit(ref, submit) =>
            supervisor.supervise(ledger.submit(submit).flatMap(result => events.offer(Edge.Event.Returned(ref, result)))).void
          case Edge.Effect.WakeAt(ref, at) =>
            val wake = IO.monotonic.flatMap(m => IO.sleep((at.millis - m.toMillis).max(0L).millis)) >> events.offer(Edge.Event.Woke(ref))
            supervisor.supervise(wake).void
          case Edge.Effect.Raise(raised) => alarm(raised)
      _ <- MachineLoop.run(Edge.machine(cfg), now, events, perform).background
      _ <- followLeaders(log, events, leadersEvery).background
    yield new EdgeProcess:
      def ask(request: Request): IO[Reply] =
        for
          answer <- IO.deferred[Reply]
          connection <- connections.getAndUpdate(_ + 1).map(Connection(_))
          _ <- waiting.update(_ + (connection -> answer))
          reply <- (events.offer(Edge.Event.Arrived(connection, request)) >> answer.get).guarantee(waiting.update(_ - connection))
        yield reply

  /** Tells the machine only of a change. If the producer cannot say, the machine keeps what it knew: with no brokers to
    * ask, the writes themselves fail, which it also handles.
    */
  private def followLeaders(log: LogWriter, events: Queue[IO, Edge.Event], every: FiniteDuration): IO[Nothing] =
    def go(known: Option[CommandLog.Leaders]): IO[Nothing] =
      log.leaders.attempt
        .flatMap:
          case Right(leaders) if !known.contains(leaders) => events.offer(Edge.Event.LeadersChanged(leaders)).as(Some(leaders))
          case _ => IO.pure(known)
        .flatMap(next => IO.sleep(every) >> go(next))
    go(None)

  extension [A](pair: (IO[A], IO[A]))
    private def mapN2[B](f: (A, A) => B): IO[B] = pair._1.flatMap(a => pair._2.map(f(a, _)))
