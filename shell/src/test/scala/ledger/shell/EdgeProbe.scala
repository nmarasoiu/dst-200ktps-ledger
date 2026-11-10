package ledger.shell

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.LockSupport
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import ledger.core.*
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Not a test: the real edge process in memory, with stand-ins for Kafka and TigerBeetle that never refuse, to see what
  * the edge itself costs per request. No HTTP. Arguments: the stand-ins (`instant`, `threads` or `blocking`), then how
  * many callers keep one request each in flight. Run it with plain `java`, after `sbt shell/writeClasspath`
  * `shell/Test/compile`, on `target/laptop.classpath` plus `target/out/jvm/scala-3.9.0/shell/test-classes`.
  */
object EdgeProbe extends IOApp:

  /** Completes from a thread of its own everything handed to it since its last round, as the TigerBeetle client and the
    * Kafka producer each do.
    */
  private final class Courier(name: String, every: FiniteDuration):
    private val handed = new ConcurrentLinkedQueue[() => Unit]
    Thread.ofPlatform().daemon().name(name).start: () =>
      while true do
        LockSupport.parkNanos(every.toNanos)
        var next = handed.poll()
        while next != null do
          next()
          next = handed.poll()
    : Unit

    def hand[A](answer: A): IO[A] = IO.async_[A](cb => handed.add(() => cb(Right(answer))): Unit)

  private def ports(mode: String): (LogWriter, LedgerCalls) =
    def created(call: Ledger.Submit) = Ledger.Result.Returned(call.transfers.map(_ => TbStatus.Created))
    def writer(written: IO[CommandLog.Written]): LogWriter = new LogWriter:
      def append(a: CommandLog.Append): IO[CommandLog.Written] = written
      def leaders: IO[CommandLog.Leaders] = IO.pure(CommandLog.Leaders(Set.empty))
    mode match
      case "instant" => (writer(IO.pure(CommandLog.Written.Acknowledged)), call => IO.pure(created(call)))
      case _ =>
        val (producer, client) = (Courier("producer", 1.milli), Courier("client", 1.milli))
        // fs2-kafka hands each record to the producer inside `blocking` (`KafkaProducer.produceRecord`).
        val send = if mode == "blocking" then IO.blocking(()) else IO.unit
        (writer(send >> producer.hand(CommandLog.Written.Acknowledged)), call => client.hand(created(call)))

  def run(args: List[String]): IO[ExitCode] =
    val mode = args.headOption.getOrElse("threads")
    val callers = args.lift(1).flatMap(_.toIntOption).getOrElse(1024)
    val (warmup, measure) = (15.seconds, 20.seconds)
    val (log, ledger) = ports(mode)
    val replies = new ConcurrentHashMap[String, AtomicLong]
    val (stop, done) = (new AtomicBoolean(false), new AtomicLong)
    val t0 = System.currentTimeMillis
    def authorize(c: Int, n: Long) = Request.Authorize(ClientKey(WallTime(t0 + n), s"probe-$c-$n"), AccountId.of(1), AccountId.of(2), U128.of(10), 7)
    val os = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
    val threads = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    def sample = IO(
      (System.nanoTime, done.get, os.getProcessCpuTime, threads.getTotalThreadAllocatedBytes,
        ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum),
    )
    EdgeProcess.resource(Settings("", Vector.empty).edge, log, ledger, _ => IO.unit).use: edge =>
      def caller(c: Int): IO[Unit] =
        def go(n: Long): IO[Unit] =
          if stop.get then IO.unit
          else
            edge.ask(authorize(c, n)).flatMap(r => IO(replies.computeIfAbsent(r.toString, _ => new AtomicLong).incrementAndGet())) >>
              IO(done.incrementAndGet()) >> go(n + 1)
        go(0)
      for
        load <- (0 until callers).toList.parTraverse_(caller).start
        _ <- IO.sleep(warmup)
        (t1, n1, cpu1, bytes1, gc1) <- sample
        _ <- IO.sleep(measure)
        (t2, n2, cpu2, bytes2, gc2) <- sample
        _ <- IO(stop.set(true))
        _ <- load.join
        seconds = (t2 - t1) / 1e9
        requests = (n2 - n1).toDouble
        _ <- IO.println(
          f"$mode, $callers callers: ${requests / seconds}%,.0f requests/s, ${(cpu2 - cpu1) / 1e9 / seconds}%.2f cores busy, " +
            f"${(cpu2 - cpu1) / 1e3 / requests}%.1f µs of CPU and ${(bytes2 - bytes1) / requests}%,.0f bytes allocated per request, " +
            f"GC ${(gc2 - gc1) / seconds}%.0f ms/s",
        )
        _ <- IO.println(replies.asScala.toVector.sortBy(-_._2.get).map((r, n) => s"  ${n.get} $r").mkString("\n"))
      yield ExitCode.Success
