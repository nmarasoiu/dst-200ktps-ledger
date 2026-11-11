//! Clocks, times, spans of time, and the one shape every process has. Port of `Machine.scala`.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;
use core::ops::{Add, Div, Sub};

use crate::mark::Mark;

mod sealed {
    pub trait Sealed {}
}

/// Whose clock a time was read from. Times from different clocks cannot be compared, so safety cannot come to
/// depend on two clocks agreeing (README section 8, clocks).
pub trait Clock: sealed::Sealed {}

/// The edge's monotonic clock, which times the 1.5 s deadline.
pub enum EdgeClock {}
/// A partition leader's clock, stamped on each record as its append time.
pub enum LogClock {}
/// The resolver's own clock, used only for a partition with no newer records.
pub enum ResolverClock {}
/// Calendar time in epoch milliseconds, as a wall clock reads it: the caller's, for a key's creation time; an
/// edge's, to place a key's period; and the period schedule itself. These are compared on purpose, and the fence
/// keeps clocks that disagree safe (README section 4, one cluster per period).
pub enum WallClock {}
/// The one clock of a history: the simulator's own time, or the workload driver's clock in real runs.
pub enum HistoryClock {}

impl sealed::Sealed for EdgeClock {}
impl sealed::Sealed for LogClock {}
impl sealed::Sealed for ResolverClock {}
impl sealed::Sealed for WallClock {}
impl sealed::Sealed for HistoryClock {}
impl Clock for EdgeClock {}
impl Clock for LogClock {}
impl Clock for ResolverClock {}
impl Clock for WallClock {}
impl Clock for HistoryClock {}

/// A time in milliseconds, read from clock `C`: an `i64` at run time, while times from different clocks stay
/// different types.
pub struct At<C: Clock> {
    millis: i64,
    clock: PhantomData<fn() -> C>,
}

pub type EdgeTime = At<EdgeClock>;
pub type LogTime = At<LogClock>;
pub type ResolverTime = At<ResolverClock>;
pub type WallTime = At<WallClock>;
pub type HistoryTime = At<HistoryClock>;

impl<C: Clock> At<C> {
    pub const fn new(millis: i64) -> Self {
        At { millis, clock: PhantomData }
    }

    pub const fn millis(self) -> i64 {
        self.millis
    }
}

impl At<LogClock> {
    /// The one place two clocks meet: an idle partition, judged by the resolver's clock (README section 8,
    /// stragglers). Skew there changes only how soon the resolver acts.
    pub const fn on_resolver_clock(self) -> ResolverTime {
        At::new(self.millis)
    }
}

impl<C: Clock> Clone for At<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C: Clock> Copy for At<C> {}

impl<C: Clock> PartialEq for At<C> {
    fn eq(&self, other: &Self) -> bool {
        self.millis == other.millis
    }
}

impl<C: Clock> Eq for At<C> {}

impl<C: Clock> PartialOrd for At<C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<C: Clock> Ord for At<C> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.millis.cmp(&other.millis)
    }
}

impl<C: Clock> Hash for At<C> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.millis.hash(state);
    }
}

impl<C: Clock> fmt::Debug for At<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.millis)
    }
}

impl<C: Clock> fmt::Display for At<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.millis)
    }
}

/// A span adds its whole milliseconds, as Scala's `toMillis` does.
impl<C: Clock> Add<Span> for At<C> {
    type Output = At<C>;

    fn add(self, d: Span) -> At<C> {
        At::new(self.millis + d.as_millis())
    }
}

/// The time between two readings of the same clock.
impl<C: Clock> Sub for At<C> {
    type Output = Span;

    fn sub(self, that: At<C>) -> Span {
        Span::from_millis(self.millis - that.millis)
    }
}

/// A signed span of time in nanoseconds: the port of Scala's `FiniteDuration`, which compares by nanoseconds and
/// may be negative, as the difference of two readings can be.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Span {
    nanos: i64,
}

impl Span {
    pub const ZERO: Span = Span { nanos: 0 };

    pub const fn from_nanos(nanos: i64) -> Span {
        Span { nanos }
    }

    pub const fn from_millis(millis: i64) -> Span {
        Span { nanos: millis * 1_000_000 }
    }

    pub const fn from_secs(secs: i64) -> Span {
        Span { nanos: secs * 1_000_000_000 }
    }

    pub const fn from_hours(hours: i64) -> Span {
        Span::from_secs(hours * 3600)
    }

    pub const fn as_nanos(self) -> i64 {
        self.nanos
    }

    /// Whole milliseconds, truncated toward zero.
    pub const fn as_millis(self) -> i64 {
        self.nanos / 1_000_000
    }
}

impl Add for Span {
    type Output = Span;

    fn add(self, that: Span) -> Span {
        Span { nanos: self.nanos + that.nanos }
    }
}

/// Integer division of the nanoseconds, as `FiniteDuration./(Long)` does.
impl Div<i64> for Span {
    type Output = Span;

    fn div(self, divisor: i64) -> Span {
        Span { nanos: self.nanos / divisor }
    }
}

impl fmt::Debug for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nanos % 1_000_000 == 0 {
            write!(f, "{} milliseconds", self.nanos / 1_000_000)
        } else {
            write!(f, "{} nanoseconds", self.nanos)
        }
    }
}

/// One step of a machine: the new state, the effects the shell must perform, and the coverage marks hit.
#[derive(Clone, Debug, PartialEq)]
pub struct Step<S, F> {
    pub state: S,
    pub effects: Vec<F>,
    pub marks: Vec<Mark>,
}

impl<S, F> Step<S, F> {
    pub fn new(state: S) -> Self {
        Step { state, effects: Vec::new(), marks: Vec::new() }
    }

    pub fn with(state: S, effects: Vec<F>, marks: Vec<Mark>) -> Self {
        Step { state, effects, marks }
    }

    pub fn flat_map<S2>(self, f: impl FnOnce(S) -> Step<S2, F>) -> Step<S2, F> {
        let next = f(self.state);
        let mut effects = self.effects;
        effects.extend(next.effects);
        let mut marks = self.marks;
        marks.extend(next.marks);
        Step { state: next.state, effects, marks }
    }

    pub fn mark(mut self, m: Mark) -> Self {
        self.marks.push(m);
        self
    }
}

type StepFn<S, E, F, T> = dyn Fn(S, T, E) -> Step<S, F>;

/// A process as the interpreters run it: the state it starts with, holding nothing, and its step (README section 11,
/// processes). `T` is what the process reads from its clocks at each step. Every process has this one shape, so the
/// simulator runs them alike: a crash drops the state and restarts from `initial`, a pause holds back the events,
/// and a planted bug rewrites the effects.
pub struct Machine<S, E, F, T> {
    pub initial: S,
    step: Box<StepFn<S, E, F, T>>,
}

impl<S, E, F, T> Machine<S, E, F, T> {
    pub fn new(initial: S, step: impl Fn(S, T, E) -> Step<S, F> + 'static) -> Self {
        Machine { initial, step: Box::new(step) }
    }

    pub fn step(&self, s: S, now: T, e: E) -> Step<S, F> {
        (self.step)(s, now, e)
    }
}

impl<S: fmt::Debug, E, F, T> fmt::Debug for Machine<S, E, F, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Machine").field("initial", &self.initial).finish_non_exhaustive()
    }
}
