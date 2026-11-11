//! Periods and their schedule (README section 4, one cluster per period). Port of `Periods.scala`.

use alloc::vec::Vec;

use crate::machine::WallTime;

/// A period of the schedule, which names the TigerBeetle cluster that holds the holds of its keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Period(u32);

impl Period {
    pub const fn new(index: u32) -> Period {
        Period(index)
    }

    pub const fn index(self) -> u32 {
        self.0
    }
}

/// The period schedule, fixed in advance like the ID layout. Period 0 covers every key before the first switch;
/// period i starts at `switches[i - 1]`. With no switches there is one period, as in stages 1 to 7.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeriodSchedule {
    switches: Vec<WallTime>,
}

impl PeriodSchedule {
    pub const SINGLE: PeriodSchedule = PeriodSchedule { switches: Vec::new() };

    /// Panics unless the switches increase, as the Scala `require` throws.
    pub fn new(switches: Vec<WallTime>) -> PeriodSchedule {
        assert!(switches.windows(2).all(|w| w[0] < w[1]), "switches must increase: {switches:?}");
        PeriodSchedule { switches }
    }

    pub fn switches(&self) -> &[WallTime] {
        &self.switches
    }

    /// The period of a key's creation time: the time comes from the key, never from a clock at the edge.
    pub fn period_of(&self, key_time: WallTime) -> Period {
        let count = self.switches.iter().filter(|s| **s <= key_time).count();
        Period(u32::try_from(count).expect("fewer than 2^32 periods"))
    }

    pub fn has_started(&self, p: Period, now: WallTime) -> bool {
        p.0 == 0 || now >= self.switches[p.0 as usize - 1]
    }

    pub fn has_ended(&self, p: Period, now: WallTime) -> bool {
        (p.0 as usize) < self.switches.len() && now >= self.switches[p.0 as usize]
    }
}
