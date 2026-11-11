//! Transfer IDs, client keys and the permanent ID layout (README section 7). Port of `Ids.scala`.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use sha2::{Digest, Sha256};

use crate::machine::WallTime;

/// A TigerBeetle transfer ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TransferId(u128);

impl TransferId {
    pub const ZERO: TransferId = TransferId(0);

    pub const fn new(value: u128) -> TransferId {
        TransferId(value)
    }

    pub const fn value(self) -> u128 {
        self.0
    }

    /// 32 lowercase hex digits, as the Scala `U128.hex` writes it.
    pub fn hex(self) -> String {
        format!("{:032x}", self.0)
    }
}

impl fmt::Debug for TransferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:032x}", self.0)
    }
}

impl fmt::Display for TransferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// The caller's idempotency key. It starts with its creation time in milliseconds, from the caller's clock, and is
/// unique within the deployment for all time (README section 7).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientKey {
    created_at: WallTime,
    body: String,
}

impl ClientKey {
    /// Times that keep every ID away from 0 and from the maximum value, which TigerBeetle rejects: the time fills
    /// the high 48 bits, so it can be neither all zeros nor all ones.
    pub const MIN_TIME: i64 = 1;
    pub const MAX_TIME: i64 = (1 << 48) - 2;

    pub fn try_new(created_at: WallTime, body: impl Into<String>) -> Result<ClientKey, String> {
        let t = created_at.millis();
        if !(Self::MIN_TIME..=Self::MAX_TIME).contains(&t) {
            Err(format!("creation time {t} is outside [{}, {}]", Self::MIN_TIME, Self::MAX_TIME))
        } else {
            Ok(ClientKey { created_at, body: body.into() })
        }
    }

    /// Panics on a time outside the range, as the Scala `apply` throws.
    pub fn new(created_at: WallTime, body: impl Into<String>) -> ClientKey {
        match Self::try_new(created_at, body) {
            Ok(key) => key,
            Err(e) => panic!("{e}"),
        }
    }

    pub const fn created_at(&self) -> WallTime {
        self.created_at
    }

    pub fn body(&self) -> &str {
        &self.body
    }
}

const AUTHORIZATION_TAG: &str = "authorization";
const VOID_TAG: &str = "void";
const POST_TAG: &str = "post";

/// `A`: the key's creation time in the high 48 bits, then 80 bits of a hash of the whole key. A cancel recomputes
/// `A` from the client key, and `V` and `P` from `A`, so the layout, the hash and its input encoding can never
/// change once a hold exists. The tests pin the output with the Scala core's test vectors.
pub fn authorization(key: &ClientKey) -> TransferId {
    let t = key.created_at.millis();
    layout(t, &encode(AUTHORIZATION_TAG, &[&t.to_be_bytes(), key.body.as_bytes()]))
}

/// `V`, the void of a cancel: `A`'s time, then 80 bits of a hash of `A` under the void tag.
pub fn void(a: TransferId) -> TransferId {
    layout(time_of(a).millis(), &encode(VOID_TAG, &[&a.0.to_be_bytes()]))
}

/// `P`, the post of a capture: `A`'s time, then 80 bits of a hash of `A` under the post tag.
pub fn post(a: TransferId) -> TransferId {
    layout(time_of(a).millis(), &encode(POST_TAG, &[&a.0.to_be_bytes()]))
}

pub const fn time_of(id: TransferId) -> WallTime {
    WallTime::new((id.0 >> 80) as i64)
}

/// The hash input: the tag, then each field, each prefixed by its length as a 4-byte big-endian integer. The tag
/// keeps the inputs of different ID kinds apart, so no client key can encode to the input of a void or a post.
pub fn encode(tag: &str, fields: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in core::iter::once(tag.as_bytes()).chain(fields.iter().copied()) {
        let length = u32::try_from(part.len()).expect("a field of the hash input is shorter than 4 GiB");
        out.extend_from_slice(&length.to_be_bytes());
        out.extend_from_slice(part);
    }
    out
}

/// SHA-256, truncated to its first 80 bits, under the time.
fn layout(time: i64, input: &[u8]) -> TransferId {
    let hash = Sha256::digest(input);
    let top16 = u16::from_be_bytes([hash[0], hash[1]]) as u128;
    let mut low = [0u8; 8];
    low.copy_from_slice(&hash[2..10]);
    let hi = ((time as u128) << 16) | top16;
    TransferId((hi << 64) | u64::from_be_bytes(low) as u128)
}
