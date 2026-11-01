package ledger.sim

import ledger.core.*
import ledger.sim.Coverage.Scenario
import scala.collection.mutable
import scala.concurrent.duration.*

/** The whole pipeline under one seed (README section 11, simulator): the core's edge and resolver machines, on the
  * simulated Kafka and TigerBeetle, driven by callers that keep their contracts. It performs their effects in one
  * event loop, in one thread, writes the history of section 11, and judges the run with the invariants.
  *
  * The seed chooses the workload, every delay, what each write and call comes to, each clock's skew, and the whole
  * plan of crashes and pauses, which ends with every process healed. A run goes on past that to show the liveness
  * properties.
  */
object Pipeline:

  /** What a fault can strike. A partition's leader or the ledger stops serving while it is down, however it went. */
  enum Target derives CanEqual:
    case EdgeInstance(index: Int)
    case ResolverInstance(index: Int)
    case LeaderOf(partition: Partition)
    case Cluster

  final case class Config(
      /** What the run does beyond faults, which every scenario has (README section 11, coverage marks). */
      scenario: Scenario = Scenario.Faults,
      edges: Int = 3,
      resolvers: Int = 2,
      partitions: Int = 4,
      /** New keys a second, while keys arrive. */
      keysPerSecond: Int = 12,
      faults: FaultPlan.Config = FaultPlan.Config(HistoryTime(15000), maxGap = 900.millis, maxOutage = 3.seconds, crashChance = 0.5),
      /** Keys arrive for this long after faults stop, so timeliness is judged on requests sent to a healed pipeline. */
      calmArrivals: FiniteDuration = 10.seconds,
      /** How long the run goes on after the last key, to let every command and hold end. */
      drain: FiniteDuration = 20.seconds,
      maxSkew: FiniteDuration = 400.millis,
      /** The hold timeouts a seed chooses from. Short ones reach expiry within a run. Long ones outlast it, so a hold
        * that should not have survived is still there at the end for the invariants to see.
        */
      holdTimeouts: Vector[Int] = Vector(3, 3, 3600),
  ):
    def lastArrival: HistoryTime = faults.stopAt + calmArrivals
    def endAt: HistoryTime = lastArrival + drain

  val Deadline: FiniteDuration = 1500.millis
  val Grace: FiniteDuration = 3.seconds
  val PollEvery: FiniteDuration = 50.millis
  val PulseEvery: FiniteDuration = 100.millis
  val GroupTickEvery: FiniteDuration = 100.millis
  val MetadataEvery: FiniteDuration = 200.millis
  val MemberTimeout: FiniteDuration = 2.seconds
  val CallerTimeout: FiniteDuration = 3.seconds
  val SendTimeout: FiniteDuration = 1.second

  val AlarmProperty = "A correct pipeline raises no alarm under faults alone"

  val ledgerConfig: LedgerConfig = LedgerConfig(AccountId.of(9001), AccountId.of(9002), 99, 90, 71)

  val bounds: Liveness.Bounds = Liveness.Bounds(
    deadline = Deadline,
    answerWithin = Deadline + 1.second,
    outcomeWithin = Deadline + Grace + 5.seconds,
    settle = 6.seconds,
    expiryLag = 1.second,
  )

  final case class Run(result: RunResult, history: History, alarms: Vector[Alarm], trace: Vector[String])

  def run(seed: Long, cfg: Config = Config(), planted: Option[Planted] = None, keepTrace: Boolean = false): Run =
    new Sim(seed, cfg, planted, keepTrace).run()

  /** Everything that happens in a run is one of these, so a trace can print it. */
  private enum Ev derives CanEqual:
    case Inject(fault: Fault[Target])
    case NewKey(index: Int)
    case Act(key: Int, event: Caller.Event)
    case Send(key: Int, seq: Long, request: Request)
    case Heard(key: Int, seq: Long, reply: Reply)
    case GaveUp(key: Int, seq: Long)
    case Reaches(instance: Int, key: Int, seq: Long, request: Request)
    case ToEdge(instance: Int, life: Incarnation, event: Edge.Event)
    case ToResolver(instance: Int, life: Incarnation, event: Resolver.Event)
    case Poll(instance: Int, life: Incarnation)
    case Metadata(instance: Int, life: Incarnation)
    case Commits(member: SimKafka.MemberId, commit: CommandLog.Commit)
    case LogArrives(append: CommandLog.Append, reply: LogReply)
    /** `noFence`: a planted bug dropped the cancel's fence, and pretends to the machine that it ran. */
    case LedgerArrives(submit: Ledger.Submit, reply: LedgerReply, noFence: Boolean = false)
    case Pulse
    case GroupTick
    case CloseAccount(id: AccountId)
    /** A caller that breaks its contracts, beside the machine that keeps them. */
    case Rogue(key: Int, kind: Int)
    case RogueSend(request: Request)
    case RogueHeard(request: Request, reply: Reply)

  /** The ways a ledger can be set up wrong. */
  private enum Misconfiguration derives CanEqual:
    case FenceAccountsMissing, FenceFieldsDiffer, FenceInvalid, AccountClosed

  /** A connection opened by a rogue caller, which no machine waits on. */
  private val RogueKey = -1

  private enum LogReply derives CanEqual:
    case ToEdgeRef(instance: Int, life: Incarnation, ref: Edge.Ref, record: Record)
    case ToResolverRef(instance: Int, life: Incarnation, ref: Resolver.Ref)

  private enum LedgerReply derives CanEqual:
    case ToEdgeRef(instance: Int, life: Incarnation, ref: Edge.Ref)
    case ToResolverRef(instance: Int, life: Incarnation, ref: Resolver.Ref)
    case Nobody

  /** A request a caller has open on an edge's connection. `record` indexes the edge's record of it. */
  private final case class Open(key: Int, seq: Long, record: Int)

  private final class KeyState(var current: Caller.State):
    var seq: Long = 0L
    var answered: Boolean = true
    var request: Option[Request] = None
    var resends: Int = 0

  private val BaseWall = 1_700_000_000_000L
  private val BaseLedgerNanos = 1_000_000_000_000L
  private val NanosPerMilli = 1_000_000L

  private final class Sim(seed: Long, cfg: Config, planted: Option[Planted], keepTrace: Boolean):
    private val rng = new Rng(seed)
    private val loop = new EventLoop[Ev]
    private val marks = new MarkCounts
    private val trace = new Trace(keepTrace)
    private val kafka = new SimKafka(cfg.partitions, MemberTimeout.toMillis)

    private val schedule =
      if cfg.scenario == Scenario.PeriodSwitch then PeriodSchedule(Vector(WallTime(BaseWall + rng.between(3000, 12000)))) else PeriodSchedule.Single
    private val ledgers = Vector.tabulate(schedule.switches.size + 1)(i => new SimTigerBeetle(Period(i)))

    private val misconfiguration =
      if cfg.scenario == Scenario.MisconfiguredLedger then Some(rng.pick(Misconfiguration.values.toVector)) else None

    /** What an instance puts in its fences. An instance is told apart by `who`, so instances can disagree. */
    private def fencedBy(who: Int): LedgerConfig = misconfiguration match
      case Some(Misconfiguration.FenceAccountsMissing) => ledgerConfig.copy(fenceDebit = AccountId.of(9100))
      case Some(Misconfiguration.FenceFieldsDiffer) => ledgerConfig.copy(fenceCode = 90 + who)
      case Some(Misconfiguration.FenceInvalid) => ledgerConfig.copy(fenceCode = 0)
      case _ => ledgerConfig

    private val holdTimeoutSeconds = rng.pick(cfg.holdTimeouts)
    private def edgeConfig(i: Int) = Edge.Config(
      EdgeRequest.Config(
        deadline = Deadline,
        handOffBudget = 1.second,
        lateResultWait = 1.second,
        holdTimeoutSeconds = holdTimeoutSeconds,
        periods = schedule,
        ledger = fencedBy(i),
      ),
      Admission.Config(maxInFlight = 64, maxLedgerLatency = 800.millis, partitions = cfg.partitions),
    )
    private def resolverConfig(i: Int) =
      ResolverPartition.Config(Deadline, Grace, retryBackoff = 300.millis, schedule, fencedBy(cfg.edges + i))

    private val edgeMonotonic = Vector.fill(cfg.edges)(SkewedClock.chosen[Clock.Edge](rng, cfg.maxSkew))
    private val edgeWall = Vector.fill(cfg.edges)(SkewedClock[Clock.Wall](BaseWall + rng.between(-cfg.maxSkew.toMillis, cfg.maxSkew.toMillis)))
    private val edges = Vector.tabulate(cfg.edges): i =>
      new SimProcess[Edge.State, Edge.Event, Edge.Effect, Edge.Now](
        Edge.machine(edgeConfig(i)),
        at => Edge.Now(edgeMonotonic(i).read(at), edgeWall(i).read(at)),
        marks,
      )

    private val resolverClocks = Vector.fill(cfg.resolvers)(SkewedClock.chosen[Clock.Resolver](rng, cfg.maxSkew))
    private val resolvers = Vector.tabulate(cfg.resolvers): i =>
      new SimProcess[Resolver.State, Resolver.Event, Resolver.Effect, ResolverTime](Resolver.machine(resolverConfig(i)), resolverClocks(i).read, marks)
    private val members = new Array[SimKafka.MemberId](cfg.resolvers)

    private val leaderClocks = Array.fill(cfg.partitions)(SkewedClock.chosen[Clock.Log](rng, cfg.maxSkew))
    private val callerWall = SkewedClock[Clock.Wall](BaseWall + rng.between(-cfg.maxSkew.toMillis, cfg.maxSkew.toMillis))

    private val keys = mutable.ArrayBuffer.empty[KeyState]
    private val open = mutable.LinkedHashMap.empty[(Int, Connection), Open]
    private var nextConnection = 0L
    private val told = Array.fill(cfg.edges)(CommandLog.Leaders(Set.empty))
    private var ledgerUp = true
    private val ledgerWaiting = mutable.ArrayBuffer.empty[(Ledger.Submit, LedgerReply, Boolean)]

    private val callerRecords = Vector.newBuilder[CallerRecord]
    private val logRecords = Vector.newBuilder[LogRecord]
    private val edgeRecords = mutable.ArrayBuffer.empty[EdgeRecord]
    private val alarms = Vector.newBuilder[Alarm]

    // The seed also chooses how rough this run's weather is, so calm runs and bad ones both happen.
    private val lossChance = rng.pick(Vector(0.0, 0.01, 0.05))
    private val slowChance = rng.pick(Vector(0.0, 0.02, 0.1))

    private def millis(low: Long, high: Long): FiniteDuration = rng.between(low, high).millis

    /** Faults stop with the fault phase, the network's among them. */
    private def stormy: Boolean = loop.now < cfg.faults.stopAt

    /** Whether this message, write or call runs into trouble. */
    private def lost(): Boolean = stormy && rng.chance(lossChance)

    /** A hop's delay, during the fault phase now and then much longer than usual. */
    private def hop(): FiniteDuration = if stormy && rng.chance(slowChance) then millis(200, 2500) else millis(1, 15)

    private val funded = Vector.tabulate(4)(i => AccountId.of(1000L + i))
    private val unfunded = AccountId.of(2000)
    private val merchant = AccountId.of(3000)
    private val AccountLedger = 7

    private def setUp(): Unit =
      val accounts =
        (funded :+ unfunded).map(id => Ledger.NewAccount(id, AccountLedger, 70, debitsMustNotExceedCredits = true)) ++ Vector(
          Ledger.NewAccount(merchant, AccountLedger, 70),
          Ledger.NewAccount(AccountId.of(4000), AccountLedger, 70),
          Ledger.NewAccount(ledgerConfig.fenceDebit, ledgerConfig.fenceLedger, 90),
          Ledger.NewAccount(ledgerConfig.fenceCredit, ledgerConfig.fenceLedger, 90),
        )
      ledgers.zipWithIndex.foreach: (ledger, cluster) =>
        val funding = funded.zipWithIndex.map: (id, i) =>
          Transfer(TransferId(U128(cluster + 1L, i + 1L)), AccountId.of(4000), id, U128.of(20_000), TransferId.Zero, AccountLedger, 72, Set.empty, 0)
        ledger.createAccounts(ledgerNow, accounts)
        val statuses = ledger.createTransfers(ledgerNow, funding)
        require(statuses.forall(_ == TbStatus.Created), s"funding failed: $statuses")

    private def ledgerTime(t: HistoryTime): Long = BaseLedgerNanos + t.millis * NanosPerMilli
    private def ledgerNow: Long = ledgerTime(loop.now)

    private def leaderTime(p: Partition): LogTime = leaderClocks(p.index).read(loop.now)

    def run(): Run =
      setUp()
      FaultPlan.generate(rng, targets, cfg.faults).foreach(f => loop.schedule(f.at, Ev.Inject(f.fault)))
      resolvers.indices.foreach: i =>
        members(i) = kafka.join(loop.now.millis)
        loop.after(millis(0, 50), Ev.Poll(i, resolvers(i).incarnation))
      edges.indices.foreach(i => loop.after(millis(0, 200), Ev.Metadata(i, edges(i).incarnation)))
      loop.after(PulseEvery, Ev.Pulse)
      loop.after(GroupTickEvery, Ev.GroupTick)
      loop.after(nextArrival(), Ev.NewKey(0))
      if misconfiguration.contains(Misconfiguration.AccountClosed) then
        loop.schedule(HistoryTime(rng.between(2000, 8000)), Ev.CloseAccount(rng.pick(funded)))
      loop.run(cfg.endAt): event =>
        trace.record(loop.now, event)
        handle(event)
      val history = History(
        callerRecords.result(),
        logRecords.result(),
        edgeRecords.toVector,
        snapshot,
        Some(cfg.faults.stopAt),
        cfg.endAt,
      )
      val raised = alarms.result()
      val alarmed = raised.distinct.filterNot(allowed).map(a => Violation(AlarmProperty, a.toString))
      // A misconfigured ledger is promised safety only: a fence it cannot take leaves its commands without an end.
      val liveness = if misconfiguration.isEmpty then Liveness.check(history, bounds) else Vector.empty
      val safety = if cfg.scenario == Scenario.ContractBreakingCaller then Invariants.check(history) else Invariants.checkKeptContracts(history)
      val violations = safety ++ liveness ++ alarmed
      Run(RunResult(violations, marks, trace.fingerprint), history, raised, trace.lines)

    private def snapshot: LedgerSnapshot =
      val all = ledgers.map(_.snapshot)
      LedgerSnapshot(
        all.flatMap(_.transfers),
        all.flatMap(_.accounts),
        all.flatMap(_.scannedAt).toMap,
        Some(all.flatMap(_.expiries.getOrElse(Map.empty)).toMap),
        ledgers.indices.map(i => Period(i) -> ledgerTime(cfg.faults.stopAt)).toMap,
      )

    private def allowed(alarm: Alarm): Boolean = cfg.scenario match
      case Scenario.Faults | Scenario.PeriodSwitch => false
      case Scenario.MisconfiguredLedger => true
      case Scenario.ContractBreakingCaller =>
        alarm match
          case _: Alarm.KeyReused => true
          case _ => false

    private def targets: Vector[Target] =
      edges.indices.toVector.map(Target.EdgeInstance(_)) ++ resolvers.indices.toVector.map(Target.ResolverInstance(_)) ++
        (0 until cfg.partitions).toVector.map(i => Target.LeaderOf(Partition(i))) :+ Target.Cluster

    private def nextArrival(): FiniteDuration = millis(1, 2000L / cfg.keysPerSecond)

    private def handle(event: Ev): Unit = event match
      case Ev.Inject(fault) => inject(fault)
      case Ev.NewKey(index) => newKey(index)
      case Ev.Act(key, e) => step(key, e)
      case Ev.Send(key, seq, request) => send(key, seq, request)
      case Ev.Heard(key, seq, reply) => heard(key, seq, reply)
      case Ev.GaveUp(key, seq) => gaveUp(key, seq)
      case Ev.Reaches(instance, key, seq, request) => reaches(instance, key, seq, request)
      case Ev.ToEdge(i, life, e) => performEdge(i, edges(i).deliver(life, loop.now, e))
      case Ev.ToResolver(i, life, e) => performResolver(i, resolvers(i).deliver(life, loop.now, e))
      case Ev.Poll(i, life) => poll(i, life)
      case Ev.Metadata(i, life) => metadata(i, life)
      case Ev.Commits(member, commit) =>
        val _ = kafka.commit(member, commit)
      case Ev.LogArrives(append, reply) => logArrives(append, reply)
      case Ev.LedgerArrives(submit, reply, noFence) => ledgerArrives(submit, reply, noFence)
      case Ev.Pulse =>
        if ledgerUp then ledgers.foreach(_.pulse(ledgerNow))
        loop.after(PulseEvery, Ev.Pulse)
      case Ev.CloseAccount(id) => ledgers.foreach(_.closeAccount(id))
      case Ev.Rogue(key, kind) => rogue(key, kind)
      case Ev.RogueSend(request) => rogueSend(request)
      case Ev.RogueHeard(request, reply) => rogueHeard(request, reply)
      case Ev.GroupTick =>
        kafka.expireMembers(loop.now.millis)
        loop.after(GroupTickEvery, Ev.GroupTick)

    // Faults.

    private def inject(fault: Fault[Target]): Unit = fault match
      case Fault.Crash(Target.EdgeInstance(i)) =>
        edges(i).crash()
        // The connections go with the process: their callers see a connection error.
        open.keysIterator.filter(_._1 == i).toVector.foreach: k =>
          val o = open(k)
          open.remove(k)
          loop.after(hop(), Ev.GaveUp(o.key, o.seq))
      case Fault.Restart(Target.EdgeInstance(i)) =>
        edges(i).restart()
        told(i) = CommandLog.Leaders(Set.empty)
        loop.after(millis(0, 200), Ev.Metadata(i, edges(i).incarnation))
      case Fault.Pause(Target.EdgeInstance(i)) => edges(i).pause()
      case Fault.Resume(Target.EdgeInstance(i)) => performEdge(i, edges(i).resume(loop.now))
      case Fault.Crash(Target.ResolverInstance(i)) => resolvers(i).crash()
      case Fault.Restart(Target.ResolverInstance(i)) =>
        resolvers(i).restart()
        members(i) = kafka.join(loop.now.millis)
        loop.after(millis(0, 50), Ev.Poll(i, resolvers(i).incarnation))
      case Fault.Pause(Target.ResolverInstance(i)) => resolvers(i).pause()
      case Fault.Resume(Target.ResolverInstance(i)) => performResolver(i, resolvers(i).resume(loop.now))
      case Fault.Crash(Target.LeaderOf(p)) => leaderDown(p)
      case Fault.Pause(Target.LeaderOf(p)) => leaderDown(p)
      case Fault.Restart(Target.LeaderOf(p)) => leaderUp(p)
      case Fault.Resume(Target.LeaderOf(p)) => leaderUp(p)
      case Fault.Crash(Target.Cluster) | Fault.Pause(Target.Cluster) => ledgerUp = false
      case Fault.Restart(Target.Cluster) | Fault.Resume(Target.Cluster) =>
        ledgerUp = true
        val waiting = ledgerWaiting.toVector
        ledgerWaiting.clear()
        waiting.foreach((submit, reply, noFence) => loop.after(millis(1, 20), Ev.LedgerArrives(submit, reply, noFence)))

    private def leaderDown(p: Partition): Unit = kafka.setLeader(p, present = false)

    /** The new leader has its own clock (README section 8, clocks). */
    private def leaderUp(p: Partition): Unit =
      leaderClocks(p.index) = SkewedClock.chosen[Clock.Log](rng, cfg.maxSkew)
      kafka.setLeader(p, present = true)

    /** A producer's metadata refreshes now and then, so an edge learns of a lost leader late, and in order. */
    private def metadata(i: Int, life: Incarnation): Unit =
      if life == edges(i).incarnation then
        if kafka.leaders != told(i) then
          told(i) = kafka.leaders
          performEdge(i, edges(i).deliver(life, loop.now, Edge.Event.LeadersChanged(told(i))))
        loop.after(MetadataEvery, Ev.Metadata(i, life))

    // The callers: the contracts are the machine's, and these are the choices it leaves open.

    private def newKey(index: Int): Unit =
      val key = ClientKey(callerWall.read(loop.now), s"key-$index")
      val debit = if rng.chance(0.1) then unfunded else rng.pick(funded)
      val authorization: Request.Authorize = Request.Authorize(key, debit, merchant, U128.of(rng.between(1, 300)), AccountLedger)
      val started = Caller.start(authorization)
      keys += new KeyState(started.state)
      carryOut(keys.size - 1, started)
      if cfg.scenario == Scenario.ContractBreakingCaller && rng.chance(0.4) then
        loop.after(millis(0, 6000), Ev.Rogue(keys.size - 1, rng.between(0, 7).toInt))
      if loop.now < cfg.lastArrival then loop.after(nextArrival(), Ev.NewKey(index + 1))

    private def resendsInstead: Boolean = planted.contains(Planted.CallerTrustsAResend)

    private def step(key: Int, e: Caller.Event): Unit =
      carryOut(key, Caller.step(keys(key).current, e))

    private def carryOut(key: Int, next: Step[Caller.State, Caller.Effect]): Unit =
      val k = keys(key)
      k.current = next.state
      marks.add(next.marks)
      val goesQuiet = planted.contains(Planted.CallerStopsCancelling) && next.marks.contains(Mark.CallerResendsCancel)
      if !goesQuiet then
        next.effects.foreach:
          case Caller.Effect.Send(request) =>
            k.seq += 1
            k.answered = false
            val sent = if resendsInstead && request.kind == Kind.Cancel then k.current.authorization else request
            k.request = Some(sent)
            loop.after(millis(0, 150), Ev.Send(key, k.seq, sent))
      choose(key)

    /** What the machine leaves to the caller, once it is the caller's move. */
    private def choose(key: Int): Unit =
      val k = keys(key)
      k.current.phase match
        case Caller.Phase.Refused =>
          val again = k.resends < 3 && rng.chance(0.7)
          if again then k.resends += 1
          loop.after(millis(10, 500), Ev.Act(key, if again then Caller.Event.Resend else Caller.Event.Stop))
        case Caller.Phase.Approved =>
          if rng.chance(0.2) then loop.after(millis(0, 100), Ev.Act(key, Caller.Event.Stop))
          else
            val amount = k.current.authorization.amount.lo
            val capture = rng.between(0, 9) match
              case 0 => CaptureAmount.Partial(amount + 1)
              case n if n < 4 && amount > 1 => CaptureAmount.Partial(rng.between(1, amount - 1))
              case _ => CaptureAmount.Full
            val late = rng.chance(0.15)
            val delay = if late then (holdTimeoutSeconds.toLong.min(5L) * 1000L + rng.between(0, 1500)).millis else millis(0, 1000)
            loop.after(delay, Ev.Act(key, Caller.Event.Capture(capture)))
        case _ =>

    private def send(key: Int, seq: Long, request: Request): Unit =
      callerRecords += CallerRecord.Sent(loop.now, request)
      loop.after(CallerTimeout, Ev.GaveUp(key, seq))
      if !lost() then
        val edge = rng.between(0, cfg.edges - 1).toInt
        val resends = planted.contains(Planted.HopResendsAnAuthorization) && request.kind == Kind.Authorization
        if resends then
          // The hop's first copy goes on, its answer lost on the way back, and the hop tries again on its own.
          loop.after(hop(), Ev.Reaches(edge, key, Planted.AnswerLost, request))
          loop.after(millis(100, 400), Ev.Reaches(rng.between(0, cfg.edges - 1).toInt, key, seq, request))
        else loop.after(hop(), Ev.Reaches(edge, key, seq, request))

    // The caller that breaks its contracts, in the ways the ledger settles safely (README section 11, coverage marks).

    private val rogueCancelsAnswered = mutable.HashSet.empty[ClientKey]

    private def rogue(key: Int, kind: Int): Unit =
      val authorization = keys(key).current.authorization
      def freshKey = ClientKey(callerWall.read(loop.now), s"rogue-$key")
      def now(request: Request): Unit = loop.after(Duration.Zero, Ev.RogueSend(request))
      def later(request: Request): Unit = loop.after(millis(200, 1500), Ev.RogueSend(request))
      kind match
        case 0 => now(authorization)
        case 1 => now(authorization.copy(amount = U128.of(authorization.amount.lo + 1)))
        case 2 => now(authorization.copy(key = freshKey, ledger = 0))
        case 3 => now(Request.Capture(authorization.key, CaptureAmount.Full))
        case 4 => now(Request.Capture(authorization.key, CaptureAmount.Partial(1)))
        case 5 => now(Request.Cancel(authorization.key))
        case 6 =>
          val noHold = Request.Capture(freshKey, CaptureAmount.Full)
          now(noHold)
          later(noHold)
        case _ =>
          val fenced = freshKey
          now(Request.Cancel(fenced))
          later(Request.Capture(fenced, CaptureAmount.Full))

    private def rogueSend(request: Request): Unit =
      val cancelSettled = request match
        case c: Request.Cancel => rogueCancelsAnswered(c.key)
        case _ => false
      if !cancelSettled then
        callerRecords += CallerRecord.Sent(loop.now, request)
        if !lost() then loop.after(hop(), Ev.Reaches(rng.between(0, cfg.edges - 1).toInt, RogueKey, 0L, request))
        // Even this caller keeps its cancels going, or nothing could say they are acknowledged.
        if request.kind == Kind.Cancel then loop.after(CallerTimeout, Ev.RogueSend(request))

    private def rogueHeard(request: Request, reply: Reply): Unit =
      callerRecords += CallerRecord.Received(loop.now, request, reply)
      (request, reply) match
        case (c: Request.Cancel, Reply.Decided(_)) => rogueCancelsAnswered += c.key
        case _ =>

    private def heard(key: Int, seq: Long, reply: Reply): Unit =
      val k = keys(key)
      if k.seq == seq && !k.answered then
        k.answered = true
        callerRecords += CallerRecord.Received(loop.now, k.request.get, reply)
        (reply, k.current.phase) match
          case (Reply.Decided(AuthOutcome.Approved), Caller.Phase.Cancelling) if resendsInstead =>
            k.current = k.current.copy(phase = Caller.Phase.Approved)
            choose(key)
          case (Reply.Decided(_), Caller.Phase.Cancelling) if resendsInstead =>
            k.current = k.current.copy(phase = Caller.Phase.Done)
          case _ => step(key, Caller.Event.Replied(reply))
      // A reply after the key is over changes nothing, whatever it says. The machine is what makes sure of that.
      else if k.current.phase == Caller.Phase.Done then step(key, Caller.Event.Replied(reply))

    private def gaveUp(key: Int, seq: Long): Unit =
      if key != RogueKey then
        val k = keys(key)
        if k.seq == seq && !k.answered then
          k.answered = true
          callerRecords += CallerRecord.Lost(loop.now, k.request.get)
          step(key, Caller.Event.Lost)

    // The edges.

    private def reaches(i: Int, key: Int, seq: Long, request: Request): Unit =
      val edge = edges(i)
      if edge.status == SimProcess.Life.Down then loop.after(hop(), Ev.GaveUp(key, seq))
      else
        nextConnection += 1
        val connection = Connection(nextConnection)
        open((i, connection)) = Open(key, seq, edgeRecords.size)
        edgeRecords += EdgeRecord(i, loop.now, request, edgeMonotonic(i).read(loop.now), None)
        performEdge(i, edge.deliver(edge.incarnation, loop.now, Edge.Event.Arrived(connection, request)))

    private def performEdge(i: Int, effects: Vector[Edge.Effect]): Unit =
      val life = edges(i).incarnation
      val rewritten = planted.fold(effects)(bug => effects.flatMap(Planted.onEdge(bug, _, ledgerConfig)))
      rewritten.foreach:
        case Edge.Effect.Answer(connection, reply) =>
          open.remove((i, connection)).foreach: o =>
            val e = edgeRecords(o.record)
            (e.request, reply) match
              case (capture: Request.Capture, Reply.Retry) if planted.contains(Planted.EdgeCancelsWhenCaptureTimesOut) =>
                val void = Vector(Transfers.void(capture.a))
                loop.after(hop(), Ev.LedgerArrives(Ledger.Submit(schedule.periodOf(capture.key.createdAt), void), LedgerReply.Nobody))
              case _ =>
            // A planted bug can sit on an answer; the instance's record shows when it really went.
            val held = if planted.contains(Planted.EdgeMissesItsDeadline) then 2.seconds else Duration.Zero
            edgeRecords(o.record) = e.copy(answered = Some((edgeMonotonic(i).read(loop.now + held), reply)))
            if !lost() then
              val back = if o.key == RogueKey then Ev.RogueHeard(e.request, reply) else Ev.Heard(o.key, o.seq, reply)
              loop.after(held + hop(), back)
        case Edge.Effect.Append(ref, append) =>
          loop.after(hop(), Ev.LogArrives(append, LogReply.ToEdgeRef(i, life, ref, append.record)))
        case Edge.Effect.Submit(ref, submit) =>
          val noFence = planted.contains(Planted.CancelVoidsWithoutFencing) && submit.transfers.size == 2
          val sent = if noFence then submit.copy(transfers = submit.transfers.drop(1)) else submit
          loop.after(hop(), Ev.LedgerArrives(sent, LedgerReply.ToEdgeRef(i, life, ref), noFence))
        case Edge.Effect.WakeAt(ref, at) =>
          loop.schedule(edgeMonotonic(i).toHistory(at), Ev.ToEdge(i, life, Edge.Event.Woke(ref)))
        case Edge.Effect.Raise(alarm) => alarms += alarm

    // The resolvers.

    private def poll(i: Int, life: Incarnation): Unit =
      val resolver = resolvers(i)
      if life == resolver.incarnation then
        // A paused process does not poll, so the coordinator drops it, and it finds out on resuming.
        if resolver.status == SimProcess.Life.Up then
          kafka.poll(members(i), loop.now.millis, maxRecords = 200).foreach: delivery =>
            performResolver(i, resolver.deliver(life, loop.now, Resolver.Event.Delivered(delivery)))
        loop.after(PollEvery, Ev.Poll(i, life))

    private def performResolver(i: Int, effects: Vector[Resolver.Effect]): Unit =
      val life = resolvers(i).incarnation
      val rewritten = planted.fold(effects)(bug => effects.flatMap(Planted.onResolver(bug, _, resolvers(i).state, kafka, ledgerConfig)))
      rewritten.foreach:
        case Resolver.Effect.Submit(ref, submit) if planted.contains(Planted.ResolverExpiresByTheClock) && Planted.isFence(ref, submit) =>
          // It calls nothing, and hears what a fence that took `A` would say.
          loop.after(hop(), Ev.ToResolver(i, life, Resolver.Event.Returned(ref, Ledger.Result.Returned(Vector(TbStatus.Created)))))
        case Resolver.Effect.Submit(ref, submit) =>
          val noFence = planted.contains(Planted.CancelVoidsWithoutFencing) && submit.transfers.size == 2
          val sent = if noFence then submit.copy(transfers = submit.transfers.drop(1)) else submit
          loop.after(hop(), Ev.LedgerArrives(sent, LedgerReply.ToResolverRef(i, life, ref), noFence))
        case Resolver.Effect.Append(ref, _) if planted.contains(Planted.ResolverDropsOutcomes) =>
          // It never writes the outcome, and goes on as if it had.
          loop.after(hop(), Ev.ToResolver(i, life, Resolver.Event.Written(ref, CommandLog.Written.Acknowledged)))
        case Resolver.Effect.Append(ref, append) =>
          loop.after(hop(), Ev.LogArrives(append, LogReply.ToResolverRef(i, life, ref)))
        case Resolver.Effect.Commit(commit) => loop.after(hop(), Ev.Commits(members(i), commit))
        case Resolver.Effect.WakeAt(at) =>
          loop.schedule(resolverClocks(i).toHistory(at), Ev.ToResolver(i, life, Resolver.Event.TimeAdvanced))
        case Resolver.Effect.Raise(alarm) => alarms += alarm

    // The command log. A write lands whole or not at all. What its writer hears may not say which: only an
    // acknowledged write is sure to have landed (README section 5).

    private def logArrives(append: CommandLog.Append, reply: LogReply): Unit =
      val p = append.partition(cfg.partitions)
      def land(): Unit =
        val read = kafka.land(append.record, leaderTime(p))
        logRecords += LogRecord(p, read.offset, loop.now, append.record)
      if kafka.leaders.leaderless(p) then logReply(reply, CommandLog.Written.Failed, SendTimeout)
      else if lost() then
        if rng.chance(0.5) then land()
        logReply(reply, CommandLog.Written.Failed, SendTimeout)
      else
        land()
        logReply(reply, CommandLog.Written.Acknowledged, hop())

    private def logReply(reply: LogReply, written: CommandLog.Written, delay: FiniteDuration): Unit = reply match
      case LogReply.ToEdgeRef(i, life, ref, record) => loop.after(delay, Ev.ToEdge(i, life, Edge.Event.Written(ref, record, written)))
      case LogReply.ToResolverRef(i, life, ref) => loop.after(delay, Ev.ToResolver(i, life, Resolver.Event.Written(ref, written)))

    // The ledger. Its client keeps trying while the cluster is away, so a call waits and lands later. Now and then a
    // call fails without statuses, and its effect is unknown.

    private def ledgerArrives(submit: Ledger.Submit, reply: LedgerReply, noFence: Boolean): Unit =
      val ledger = ledgers(submit.cluster.index)
      if !ledgerUp then ledgerWaiting += ((submit, reply, noFence))
      else if lost() then
        if rng.chance(0.5) then
          val _ = ledger.createTransfers(ledgerNow, submit.transfers)
        ledgerReply(reply, Ledger.Result.Failed)
      else
        val statuses = ledger.createTransfers(ledgerNow, submit.transfers)
        ledgerReply(reply, Ledger.Result.Returned(if noFence then Planted.pretendFenced(statuses) else statuses))

    private def ledgerReply(reply: LedgerReply, result: Ledger.Result): Unit = reply match
      case LedgerReply.ToEdgeRef(i, life, ref) => loop.after(hop(), Ev.ToEdge(i, life, Edge.Event.Returned(ref, result)))
      case LedgerReply.ToResolverRef(i, life, ref) => loop.after(hop(), Ev.ToResolver(i, life, Resolver.Event.Returned(ref, result)))
      case LedgerReply.Nobody =>
