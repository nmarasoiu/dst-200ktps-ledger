package ledger.core

import java.lang.Long.{compareUnsigned, toUnsignedString}

/** An unsigned 128-bit number, the width of TigerBeetle's IDs and amounts. */
final case class U128(hi: Long, lo: Long) derives CanEqual:
  def isZero: Boolean = hi == 0L && lo == 0L
  def isMax: Boolean = hi == -1L && lo == -1L
  def toBigInt: BigInt = (BigInt(toUnsignedString(hi)) << 64) + BigInt(toUnsignedString(lo))

  /** Big-endian, 16 bytes. */
  def bytes: Array[Byte] =
    val b = new Array[Byte](16)
    for i <- 0 until 8 do
      b(i) = (hi >>> (56 - 8 * i)).toByte
      b(8 + i) = (lo >>> (56 - 8 * i)).toByte
    b

  def hex: String = f"$hi%016x$lo%016x"
  override def toString: String = s"0x$hex"

object U128:
  val Zero: U128 = U128(0L, 0L)
  val Max: U128 = U128(-1L, -1L)

  def of(n: Long): U128 =
    require(n >= 0, s"negative: $n")
    U128(0L, n)

  def fromBigInt(n: BigInt): U128 =
    require(n >= 0 && n.bitLength <= 128, s"out of range: $n")
    U128((n >> 64).toLong, n.toLong)

  given Ordering[U128] = new Ordering[U128]:
    def compare(x: U128, y: U128): Int =
      val c = compareUnsigned(x.hi, y.hi)
      if c != 0 then c else compareUnsigned(x.lo, y.lo)
