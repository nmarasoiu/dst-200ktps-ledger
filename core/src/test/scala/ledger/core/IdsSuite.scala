package ledger.core

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class IdsSuite extends ScalaCheckSuite:

  // Fixed test vectors, computed independently of this code from the layout in README section 7. The layout is
  // permanent: if one of these changes, cancels of existing holds would aim at the wrong ID. Never update them to
  // make a test pass.
  private val vectorKey = ClientKey(WallTime(1_700_000_000_000L), "card-auth-0001")
  private val vectorA = "018bcfe56800cb5ef6d202be8e8e0a72"
  private val vectorV = "018bcfe56800609fd3bb5fee3a9d3819"
  private val vectorP = "018bcfe56800ba5fd5428ff934943ba2"

  test("A for the fixed key matches its test vector"):
    assertEquals(Ids.authorization(vectorKey).value.hex, vectorA)

  test("V and P for the fixed key match their test vectors"):
    val a = Ids.authorization(vectorKey)
    assertEquals(Ids.void(a).value.hex, vectorV)
    assertEquals(Ids.post(a).value.hex, vectorP)

  test("the hash input is the length-prefixed tag, then each length-prefixed field"):
    val encoded = Ids.encode("ab", Array[Byte](7), Array.emptyByteArray)
    assertEquals(encoded.toVector, Vector[Byte](0, 0, 0, 2, 'a', 'b', 0, 0, 0, 1, 7, 0, 0, 0, 0))

  test("a key's time must keep IDs away from 0 and the maximum value"):
    assert(ClientKey.from(WallTime(0L), "k").isLeft)
    assert(ClientKey.from(WallTime(ClientKey.MaxTime + 1), "k").isLeft)
    assert(ClientKey.from(WallTime(ClientKey.MinTime), "k").isRight)
    assert(ClientKey.from(WallTime(ClientKey.MaxTime), "k").isRight)

  property("A takes the key's time in its high 48 bits; V and P take A's time"):
    forAll(Gens.key): k =>
      val a = Ids.authorization(k)
      Ids.timeOf(a) == k.createdAt && Ids.timeOf(Ids.void(a)) == k.createdAt && Ids.timeOf(Ids.post(a)) == k.createdAt

  property("no ID is 0 or the maximum value"):
    forAll(Gens.key): k =>
      val a = Ids.authorization(k)
      Vector(a, Ids.void(a), Ids.post(a)).forall(id => !id.value.isZero && !id.value.isMax)

  property("the encodings of different ID kinds never coincide"):
    forAll(Gens.key, Gens.key): (k, other) =>
      val a = Ids.authorization(other)
      val auth = Ids.encode("authorization", java.nio.ByteBuffer.allocate(8).putLong(k.createdAt.millis).array, k.body.getBytes("UTF-8"))
      val void = Ids.encode("void", a.value.bytes)
      val post = Ids.encode("post", a.value.bytes)
      !auth.sameElements(void) && !auth.sameElements(post) && !void.sameElements(post)

  property("keys with later times give higher IDs"):
    val pair = for
      t1 <- Gen.choose(ClientKey.MinTime, ClientKey.MaxTime - 1)
      t2 <- Gen.choose(t1 + 1, ClientKey.MaxTime)
      b1 <- Gen.alphaNumStr
      b2 <- Gen.alphaNumStr
    yield (ClientKey(WallTime(t1), b1), ClientKey(WallTime(t2), b2))
    forAll(pair): (earlier, later) =>
      Ordering[TransferId].lt(Ids.authorization(earlier), Ids.authorization(later))

  property("the same key always gives the same IDs"):
    forAll(Gens.key): k =>
      val copy = ClientKey(k.createdAt, k.body)
      Ids.authorization(k) == Ids.authorization(copy) && Ids.void(Ids.authorization(k)) == Ids.void(Ids.authorization(copy))

class PartitionerSuite extends munit.FunSuite:

  // Kafka's own vectors for murmur2, from `UtilsTest.testMurmur2` in the Kafka clients.
  test("murmur2 matches Kafka's test vectors"):
    val cases = Map(
      "21" -> -973932308,
      "foobar" -> -790332482,
      "a-little-bit-long-string" -> -985981536,
      "a-little-bit-longer-string" -> -1486304829,
      "lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8" -> -58897971,
      "abc" -> 479470107,
    )
    cases.foreach((input, expected) => assertEquals(Partitioner.murmur2(input.getBytes("UTF-8")), expected, input))

  test("every command about a hold lands on the hold's partition"):
    val k = Fixtures.key(1)
    val partitions = Vector(Fixtures.authCommand(k), Fixtures.cancelCommand(k), Fixtures.captureCommand(k)).map(c => Partitioner.partition(c.a, 64))
    assertEquals(partitions.distinct.size, 1)
