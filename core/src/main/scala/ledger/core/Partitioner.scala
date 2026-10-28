package ledger.core

/** A partition of the command log. An opaque `Int`: stored as a primitive. */
opaque type Partition = Int

object Partition:
  def apply(index: Int): Partition = index
  extension (p: Partition) def index: Int = p

/** An offset in a partition. An opaque `Long`: stored as a primitive. */
opaque type Offset = Long

object Offset:
  def apply(value: Long): Offset = value
  given Ordering[Offset] = Ordering.Long

  extension (o: Offset)
    def value: Long = o
    def next: Offset = o + 1

/** Kafka's default partitioner (README section 6): murmur2 of the record key, made positive, modulo the partition
  * count. The record key is `A` as 16 big-endian bytes (README section 4, three keys). Like the ID layout, it can
  * never change while the topic lives.
  */
object Partitioner:
  def recordKey(a: TransferId): Array[Byte] = a.value.bytes

  def partition(a: TransferId, partitions: Int): Partition =
    Partition((murmur2(recordKey(a)) & 0x7fffffff) % partitions)

  /** A port of `Utils.murmur2` in the Kafka clients. */
  def murmur2(data: Array[Byte]): Int =
    val length = data.length
    val m = 0x5bd1e995
    val r = 24
    var h = 0x9747b28cL.toInt ^ length
    val length4 = length >> 2
    for i <- 0 until length4 do
      val i4 = i << 2
      var k = (data(i4) & 0xff) | ((data(i4 + 1) & 0xff) << 8) | ((data(i4 + 2) & 0xff) << 16) | ((data(i4 + 3) & 0xff) << 24)
      k *= m
      k ^= k >>> r
      k *= m
      h *= m
      h ^= k
    val index = length4 << 2
    val rest = length - index
    if rest == 3 then h ^= (data(index + 2) & 0xff) << 16
    if rest >= 2 then h ^= (data(index + 1) & 0xff) << 8
    if rest >= 1 then
      h ^= data(index) & 0xff
      h *= m
    h ^= h >>> 13
    h *= m
    h ^= h >>> 15
    h
