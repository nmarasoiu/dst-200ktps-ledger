//! The port of `TypeSafetySuite`: the wrapper types cost nothing at run time, but must stay distinct types at
//! compile time. Each `compile_fail` example below is a doc test that passes only if the code fails to compile with
//! the error named.
//!
//! The valid uses compile:
//!
//! ```
//! use ledger_core::*;
//! let _ = Offset::new(1).next().value() + 1;
//! let _ = EdgeTime::new(1) < EdgeTime::new(2);
//! let _ = LogTime::new(1).on_resolver_clock() <= ResolverTime::new(5);
//! let _ = Generation::INITIAL.next() == Generation::new(1);
//! let _ = Offset::new(5) == Offset::new(5);
//! let _ = ClientKey::new(WallTime::new(1_700_000_000_000), "k").created_at() <= WallTime::new(1_800_000_000_000);
//! ```
//!
//! Times from different clocks cannot be compared:
//!
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = EdgeTime::new(1) < LogTime::new(2);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = ResolverTime::new(5) >= LogTime::new(1);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _t: ResolverTime = LogTime::new(1);
//! ```
//!
//! An offset, a generation, a partition and a period are not interchangeable:
//!
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _g: Generation = Offset::new(1);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _o: Offset = Generation::new(1);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _p: Period = Partition::new(0);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _p: Partition = Period::new(0);
//! ```
//!
//! A raw number is not one of them, nor the other way round:
//!
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _o: Offset = 5i64;
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _l: i64 = Offset::new(5);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _g: Generation = 1i64;
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _t: EdgeTime = 1500i64;
//! ```
//!
//! A wall-clock time, a history time and the edge's monotonic time are different clocks:
//!
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = WallTime::new(1) < EdgeTime::new(2);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _t: HistoryTime = WallTime::new(1);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = ClientKey::new(1_700_000_000_000i64, "k");
//! ```
//!
//! `==` between unrelated types does not compile:
//!
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = Offset::new(5) == 5i64;
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = Generation::new(1) == Offset::new(1);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = EdgeTime::new(1) == LogTime::new(1);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = Kind::Cancel == CancelOutcome::Cancelled;
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _ = TransferId::ZERO == AccountId::ZERO;
//! ```
//!
//! Transfer and account IDs are not interchangeable:
//!
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _t: TransferId = AccountId::of(1);
//! ```
//! ```compile_fail,E0308
//! use ledger_core::*;
//! let _a: AccountId = TransferId::ZERO;
//! ```
//!
//! Stricter than the Scala: an amount cannot be negative, and arithmetic on IDs does not compile.
//!
//! ```compile_fail,E0600
//! use ledger_core::*;
//! let _ = AccountId::of(-1);
//! ```
//! ```compile_fail,E0369
//! use ledger_core::*;
//! let _ = TransferId::ZERO + TransferId::ZERO;
//! ```
