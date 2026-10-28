package ledger.core

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

/** A TigerBeetle transfer ID. */
opaque type TransferId = U128

object TransferId:
  val Zero: TransferId = U128.Zero
  def apply(value: U128): TransferId = value
  extension (id: TransferId) def value: U128 = id
  given Ordering[TransferId] = summon[Ordering[U128]]
  given CanEqual[TransferId, TransferId] = CanEqual.derived

/** The caller's idempotency key. It starts with its creation time in milliseconds, from the caller's
  * clock, and is unique within the deployment for all time (README section 7).
  */
final case class ClientKey private (createdAt: WallTime, body: String) derives CanEqual

object ClientKey:
  /** Times that keep every ID away from 0 and from the maximum value, which TigerBeetle rejects: the time
    * fills the high 48 bits, so it can be neither all zeros nor all ones.
    */
  val MinTime: Long = 1L
  val MaxTime: Long = (1L << 48) - 2

  def from(createdAt: WallTime, body: String): Either[String, ClientKey] =
    val t = createdAt.millis
    if t < MinTime || t > MaxTime then Left(s"creation time $t is outside [$MinTime, $MaxTime]")
    else Right(new ClientKey(createdAt, body))

  def apply(createdAt: WallTime, body: String): ClientKey =
    from(createdAt, body).fold(e => throw IllegalArgumentException(e), identity)

/** The permanent ID layout (README section 7). A cancel recomputes `A` from the client key, and `V` and `P` from
  * `A`, so the layout, the hash and its input encoding can never change once a hold exists. `IdsSuite` pins the
  * output with test vectors.
  */
object Ids:
  private val AuthorizationTag = "authorization"
  private val VoidTag = "void"
  private val PostTag = "post"

  /** `A`: the key's creation time in the high 48 bits, then 80 bits of a hash of the whole key. */
  def authorization(key: ClientKey): TransferId =
    layout(key.createdAt.millis, encode(AuthorizationTag, long8(key.createdAt.millis), key.body.getBytes(UTF_8)))

  /** `V`, the void of a cancel: `A`'s time, then 80 bits of a hash of `A` under the void tag. */
  def void(a: TransferId): TransferId = layout(timeOf(a).millis, encode(VoidTag, a.value.bytes))

  /** `P`, the post of a capture: `A`'s time, then 80 bits of a hash of `A` under the post tag. */
  def post(a: TransferId): TransferId = layout(timeOf(a).millis, encode(PostTag, a.value.bytes))

  def timeOf(id: TransferId): WallTime = WallTime(id.value.hi >>> 16)

  /** The hash input: the tag, then each field, each prefixed by its length as a 4-byte big-endian integer. The
    * tag keeps the inputs of different ID kinds apart, so no client key can encode to the input of a void or a post.
    */
  def encode(tag: String, fields: Array[Byte]*): Array[Byte] =
    val parts = tag.getBytes(UTF_8) +: fields
    val buf = ByteBuffer.allocate(parts.map(4 + _.length).sum)
    parts.foreach(p => buf.putInt(p.length).put(p))
    buf.array

  /** SHA-256, truncated to its first 80 bits, under the time. */
  private def layout(time: Long, input: Array[Byte]): TransferId =
    val hash = ByteBuffer.wrap(MessageDigest.getInstance("SHA-256").digest(input))
    val top16 = hash.getShort(0) & 0xffffL
    TransferId(U128((time << 16) | top16, hash.getLong(2)))

  private def long8(n: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(n).array
