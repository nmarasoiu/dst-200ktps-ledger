package ledger.core

/** The opaque types cost nothing at run time, but must stay distinct types at compile time. */
class TypeSafetySuite extends munit.FunSuite:

  test("the valid uses compile"):
    assertEquals(compileErrors("Offset(1).next.value + 1L"), "")
    assertEquals(compileErrors("EdgeTime(1) < EdgeTime(2)"), "")
    assertEquals(compileErrors("LogTime(1).onResolverClock <= ResolverTime(5)"), "")
    assertEquals(compileErrors("Generation.Initial.next == Generation(1)"), "")

  test("times from different clocks cannot be compared"):
    assert(compileErrors("EdgeTime(1) < LogTime(2)").nonEmpty)
    assert(compileErrors("ResolverTime(5) >= LogTime(1)").nonEmpty)
    assert(compileErrors("val t: ResolverTime = LogTime(1)").nonEmpty)

  test("an offset, a generation, a partition and a period are not interchangeable"):
    assert(compileErrors("val g: Generation = Offset(1)").nonEmpty)
    assert(compileErrors("val o: Offset = Generation(1)").nonEmpty)
    assert(compileErrors("val p: Period = Partition(0)").nonEmpty)
    assert(compileErrors("val p: Partition = Period(0)").nonEmpty)

  test("a raw number is not one of them, nor the other way round"):
    assert(compileErrors("val o: Offset = 5L").nonEmpty)
    assert(compileErrors("val l: Long = Offset(5)").nonEmpty)
    assert(compileErrors("val g: Generation = 1L").nonEmpty)
    assert(compileErrors("val t: EdgeTime = 1500L").nonEmpty)

  test("transfer and account IDs are not interchangeable"):
    assert(compileErrors("val t: TransferId = AccountId.of(1)").nonEmpty)
    assert(compileErrors("val a: AccountId = TransferId.Zero").nonEmpty)
