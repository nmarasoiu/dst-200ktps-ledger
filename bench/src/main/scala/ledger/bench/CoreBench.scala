package ledger.bench

import java.util.concurrent.TimeUnit
import ledger.core.*
import org.openjdk.jmh.annotations.*
import scala.concurrent.duration.*

/** What the core's steps cost, one operation at a time. Commands are built fresh in each operation, as the shell
  * builds them from a request or a log record, so their IDs are hashed each time.
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class CoreBench:
  private val ledger = LedgerConfig(AccountId.of(9001), AccountId.of(9002), 9, 99, 1)
  private val holdTimeout = 7 * 24 * 3600
  private val edge = EdgeRequest.Config(1500.millis, 1200.millis, 2.seconds, holdTimeout, PeriodSchedule.Single, ledger)
  private val resolver = ResolverPartition.Config(1500.millis, 3.seconds, 500.millis, PeriodSchedule.Single, ledger)
  private val edgeProcess = Edge.Config(edge, Admission.Config(maxInFlight = 4096, maxLedgerLatency = 200.millis, partitions = 64))
  private val t0 = 1_793_145_600_000L
  private val keys = Array.tabulate(1024)(i => ClientKey(WallTime(t0 + i), s"key-$i"))
  private val requests = keys.map(k => Request.Authorize(k, AccountId.of(1), AccountId.of(2), U128.of(100), 1))
  private val ids = keys.map(Ids.authorization)
  private val created = Vector(TbStatus.Created)
  private var i = 0

  private def next(): Int =
    i = (i + 1) & 1023
    i

  /** `A` from a client key: SHA-256 over the encoded key. */
  @Benchmark def idOfKey(): TransferId = Ids.authorization(keys(next()))

  /** One authorization through the edge's flow: arrival, command acknowledged, created, outcome acknowledged. */
  @Benchmark def edgeAuthorization(): Step[EdgeRequest.State, EdgeRequest.Effect] =
    import EdgeRequest.Event.*
    val s0 = EdgeRequest.start(requests(next()), EdgeRequest.Origin.Caller, EdgeTime(0), WallTime(t0), edge)
    val s1 = EdgeRequest.step(s0.state, EdgeTime(10), CommandAcknowledged, edge)
    val s2 = EdgeRequest.step(s1.state, EdgeTime(20), LedgerReturned(created), edge)
    EdgeRequest.step(s2.state, EdgeTime(30), OutcomeAcknowledged, edge)

  /** The same authorization through the edge process: admission control, then the request's flow, with each reply
    * routed to the request by its ref.
    */
  @Benchmark def edgeProcessAuthorization(): Step[Edge.State, Edge.Effect] =
    import Edge.Event.*
    val n = next()
    val connection = Connection(n)
    val ref = Edge.Ref.Caller(connection)
    val command = Command.stamped(requests(n), holdTimeout)
    def at(t: Long) = Edge.Now(EdgeTime(t), WallTime(t0 + t))
    val acked = CommandLog.Written.Acknowledged
    val s0 = Edge.step(Edge.State.Initial, at(0), Arrived(connection, requests(n)), edgeProcess)
    val s1 = Edge.step(s0.state, at(10), Written(ref, Record.CommandRecord(command), acked), edgeProcess)
    val s2 = Edge.step(s1.state, at(20), Returned(ref, Ledger.Result.Returned(created)), edgeProcess)
    Edge.step(s2.state, at(30), Written(ref, Record.OutcomeRecord(command, AuthOutcome.Approved, Writer.Edge), acked), edgeProcess)

  /** The resolver's common case: a command, then its outcome within the grace period, so no action. */
  @Benchmark def resolverCommandThenOutcome(): Step[ResolverPartition.State, ResolverPartition.Effect] =
    import ResolverPartition.Event.Read
    val command = Command.stamped(requests(next()), holdTimeout)
    val s0 = ResolverPartition.assigned(Partition(0), Generation(1), Offset(0))
    val s1 = ResolverPartition.step(s0, ResolverTime(0), Read(Offset(0), LogTime(0), Record.CommandRecord(command)), resolver)
    val outcome = Record.OutcomeRecord(command, AuthOutcome.Approved, Writer.Edge)
    ResolverPartition.step(s1.state, ResolverTime(10), Read(Offset(1), LogTime(10), outcome), resolver)

  /** The merge of an edge's and the resolver's outcome for one hold. */
  @Benchmark def mergeTwoOutcomes(): Map[TransferId, Merge.Hold] =
    val a = ids(next())
    Merge.fold(Vector(a -> AuthOutcome.Approved, a -> AuthOutcome.Approved))
