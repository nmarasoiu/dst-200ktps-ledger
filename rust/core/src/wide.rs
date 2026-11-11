//! A signed 256-bit integer, just wide enough for sums of `u128` amounts and their negations: what the Scala
//! invariants use `BigInt` for.

use core::cmp::Ordering;
use core::fmt;
use core::ops::{Add, Neg};

/// The value `hi * 2^128 + lo`, in two's complement.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Wide {
    hi: i128,
    lo: u128,
}

impl Wide {
    pub(crate) const fn from_u128(n: u128) -> Wide {
        Wide { hi: 0, lo: n }
    }
}

impl Add for Wide {
    type Output = Wide;

    fn add(self, that: Wide) -> Wide {
        let (lo, carry) = self.lo.overflowing_add(that.lo);
        Wide { hi: self.hi + that.hi + i128::from(carry), lo }
    }
}

impl Neg for Wide {
    type Output = Wide;

    fn neg(self) -> Wide {
        let (lo, carry) = (!self.lo).overflowing_add(1);
        Wide { hi: (!self.hi).wrapping_add(i128::from(carry)), lo }
    }
}

impl Ord for Wide {
    fn cmp(&self, other: &Wide) -> Ordering {
        self.hi.cmp(&other.hi).then(self.lo.cmp(&other.lo))
    }
}

impl PartialOrd for Wide {
    fn partial_cmp(&self, other: &Wide) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Wide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.hi {
            0 => write!(f, "{}", self.lo),
            -1 if self.lo != 0 => write!(f, "-{}", (!self.lo).wrapping_add(1)),
            _ => write!(f, "Wide({}, {})", self.hi, self.lo),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Wide;

    #[test]
    fn sums_past_u128_and_below_zero_compare_as_integers() {
        let max = Wide::from_u128(u128::MAX);
        let two_max = max + max;
        assert!(two_max > max);
        assert_eq!(two_max + (-max), max);
        assert!(-max < Wide::from_u128(0));
        assert_eq!(max + (-max), Wide::from_u128(0));
        assert!(-(max + max) < -max);
        assert_eq!(format!("{:?}", -Wide::from_u128(5)), "-5");
    }
}
