package ledger.sim

import Coverage.Scenario
import ledger.core.*

class CoverageSuite extends munit.FunSuite:

  test("every mark belongs to one scenario, apart from the two no correct ledger can give"):
    val required = Scenario.values.toVector.flatMap(Coverage.required)
    assertEquals(required.size, required.distinct.size)
    assertEquals(Mark.values.toSet -- required, Set(Mark.AuthorizationUnresolved, Mark.CaptureUnresolved))

  // A correct pipeline and a caller that keeps its contracts must give an operator nothing to look at.
  test("no mark that can come with an alarm is required of faults alone"):
    val half = CaptureAmount.Partial(50)
    val statuses = TbStatus.values.toVector
    val verdicts =
      statuses.flatMap(s => Vector(Rules.authorization(s), Rules.expiry(s), Rules.capture(s, half))) ++
        (for f <- statuses; v <- statuses yield Rules.cancel(f, v))
    val withAlarm = verdicts.collect:
      case Verdict.Decided(_, m, Some(_)) => m
      case Verdict.Unresolved(m, _) => m
    assertEquals(withAlarm.toSet & Coverage.required(Scenario.Faults), Set.empty[Mark])

  test("a second copy of an authorization is not required of a caller that keeps its contracts"):
    val faults = Coverage.required(Scenario.Faults)
    assert(!faults(Mark.AuthorizationExists))
    assert(!faults(Mark.AuthorizationIdAlreadyFailed))
    assert(Coverage.required(Scenario.ContractBreakingCaller)(Mark.AuthorizationExists))

  test("marks are counted, and counts add up across runs"):
    val (one, other) = (new MarkCounts, new MarkCounts)
    one.add(Vector(Mark.EdgeAnswered, Mark.EdgeAnswered))
    other.add(Vector(Mark.EdgeAnswered, Mark.ResolverRevoked))
    one.addAll(other)
    assertEquals(one(Mark.EdgeAnswered), 3L)
    assertEquals(one(Mark.ResolverRevoked), 1L)
    assertEquals(one(Mark.CallerAbandonedHold), 0L)
    assertEquals(one.hit, Set(Mark.EdgeAnswered, Mark.ResolverRevoked))
    assert(one.report.linesIterator.exists(line => line.trim == "3  EdgeAnswered"))
