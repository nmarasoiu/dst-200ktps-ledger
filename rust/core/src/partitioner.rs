//! Partitions, offsets and Kafka's default partitioner. Port of `Partitioner.scala`.

use crate::ids::TransferId;

/// A partition of the command log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Partition(u32);

impl Partition {
    pub const fn new(index: u32) -> Partition {
        Partition(index)
    }

    pub const fn index(self) -> u32 {
        self.0
    }
}

/// An offset in a partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(i64);

impl Offset {
    pub const fn new(value: i64) -> Offset {
        Offset(value)
    }

    pub const fn value(self) -> i64 {
        self.0
    }

    pub const fn next(self) -> Offset {
        Offset(self.0 + 1)
    }
}

/// The record key: `A` as 16 big-endian bytes (README section 4, three keys).
pub fn record_key(a: TransferId) -> [u8; 16] {
    a.value().to_be_bytes()
}

/// Kafka's default partitioner (README section 6): murmur2 of the record key, made positive, modulo the partition
/// count. Like the ID layout, it can never change while the topic lives.
pub fn partition(a: TransferId, partitions: u32) -> Partition {
    Partition((murmur2(&record_key(a)) & 0x7fff_ffff) as u32 % partitions)
}

/// A port of `Utils.murmur2` in the Kafka clients, on wrapping 32-bit arithmetic as Java's `int` is.
pub fn murmur2(data: &[u8]) -> i32 {
    let length = i32::try_from(data.len()).expect("a record key shorter than 2 GiB");
    let m: i32 = 0x5bd1_e995;
    let r = 24;
    let mut h: i32 = (0x9747_b28c_u32 as i32) ^ length;
    let length4 = data.len() / 4;
    for i in 0..length4 {
        let i4 = i * 4;
        let mut k = (data[i4] as i32) | ((data[i4 + 1] as i32) << 8) | ((data[i4 + 2] as i32) << 16) | ((data[i4 + 3] as i32) << 24);
        k = k.wrapping_mul(m);
        k ^= ((k as u32) >> r) as i32;
        k = k.wrapping_mul(m);
        h = h.wrapping_mul(m);
        h ^= k;
    }
    let index = length4 * 4;
    let rest = data.len() - index;
    if rest == 3 {
        h ^= (data[index + 2] as i32) << 16;
    }
    if rest >= 2 {
        h ^= (data[index + 1] as i32) << 8;
    }
    if rest >= 1 {
        h ^= data[index] as i32;
        h = h.wrapping_mul(m);
    }
    h ^= ((h as u32) >> 13) as i32;
    h = h.wrapping_mul(m);
    h ^= ((h as u32) >> 15) as i32;
    h
}
