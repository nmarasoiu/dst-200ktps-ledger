//! The pure core of the ledger pipeline, ported from the Scala core in `core/src/main/scala/ledger/core`
//! (README section 11, functional core). It is a twin: a second implementation of the same rules and machines,
//! held to the Scala one by the same test vectors and, once the simulator is ported, by the same traces for the
//! same seeds. The Scala core stays what runs (README section 10).
//!
//! Each module mirrors one Scala file. Two things are stricter than the Scala:
//!
//! - The crate is `no_std`. Built for a target without a standard library, it cannot read a clock, a file, a
//!   socket or a thread, nor iterate a randomly seeded hash map, even through a dependency.
//! - Amounts and IDs are native `u128`, and an overflow stops the program in every build profile.
//!
//! One known difference from the Scala core: violations of the invariants come out in a fixed order and their detail
//! text differs, so compare them by property.

#![cfg_attr(not(test), no_std)]

extern crate alloc;

pub mod admission;
pub mod caller;
pub mod domain;
pub mod edge;
pub mod edge_request;
pub mod history;
pub mod ids;
pub mod invariants;
pub mod liveness;
pub mod machine;
pub mod mark;
pub mod merge;
pub mod partitioner;
pub mod periods;
pub mod ports;
pub mod record_codec;
pub mod resolver;
pub mod rules;
pub mod tb;
mod type_safety;
mod wide;

pub use domain::*;
pub use history::*;
pub use ids::{ClientKey, TransferId};
pub use machine::*;
pub use mark::Mark;
pub use partitioner::{Offset, Partition};
pub use periods::{Period, PeriodSchedule};
pub use ports::Connection;
pub use resolver::Generation;
pub use rules::Verdict;
pub use tb::*;
