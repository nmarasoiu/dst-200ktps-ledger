//! Port of `Fixtures.scala`: the configuration, keys and commands the suites share, and the generators.

#![allow(dead_code)]

use ledger_core::*;
use ledger_core::{edge_request, resolver};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;

pub const LEDGER_CFG: LedgerConfig = LedgerConfig {
    fence_debit: AccountId::of(9001),
    fence_credit: AccountId::of(9002),
    fence_ledger: 9,
    fence_code: 99,
    authorization_code: 1,
};
pub const CUSTOMER: AccountId = AccountId::of(1);
pub const MERCHANT: AccountId = AccountId::of(2);
pub const HOLD_TIMEOUT: i32 = 7 * 24 * 3600;

/// 2026-10-28T00:00:00Z.
pub const T0: i64 = 1_793_145_600_000;

pub fn key(n: i32) -> ClientKey {
    key_at(n, T0)
}

pub fn key_at(n: i32, at: i64) -> ClientKey {
    ClientKey::new(WallTime::new(at), format!("key-{n}"))
}

pub fn authorize(k: &ClientKey) -> Request {
    authorize_amount(k, 100)
}

pub fn authorize_amount(k: &ClientKey, amount: u128) -> Request {
    Request::authorize(k.clone(), CUSTOMER, MERCHANT, amount, 1)
}

pub fn authorize_request(k: &ClientKey) -> AuthorizeRequest {
    AuthorizeRequest { key: k.clone(), debit: CUSTOMER, credit: MERCHANT, amount: 100, ledger: 1 }
}

pub fn auth_command(k: &ClientKey) -> Command {
    Command::stamped(authorize(k), HOLD_TIMEOUT)
}

pub fn auth_command_amount(k: &ClientKey, amount: u128) -> Command {
    Command::stamped(authorize_amount(k, amount), HOLD_TIMEOUT)
}

pub fn authorize_command(k: &ClientKey) -> AuthorizeCommand {
    match auth_command(k) {
        Command::Authorize(c) => c,
        other => panic!("not an authorization: {other:?}"),
    }
}

pub fn cancel_command(k: &ClientKey) -> Command {
    Command::Cancel(k.clone())
}

pub fn capture_command(k: &ClientKey) -> Command {
    Command::Capture(k.clone(), CaptureAmount::Full)
}

pub fn edge_cfg() -> edge_request::Config {
    edge_request::Config::new(
        Span::from_millis(1500),
        Span::from_millis(1200),
        Span::from_secs(2),
        HOLD_TIMEOUT,
        PeriodSchedule::SINGLE,
        LEDGER_CFG,
    )
}

pub fn resolver_cfg() -> resolver::partition::Config {
    resolver::partition::Config {
        deadline: Span::from_millis(1500),
        grace: Span::from_secs(3),
        retry_backoff: Span::from_millis(500),
        periods: PeriodSchedule::SINGLE,
        ledger: LEDGER_CFG,
    }
}

/// What the resolver waits after a command, in log time, before judging it.
pub fn due() -> i64 {
    let cfg = resolver_cfg();
    (cfg.deadline + cfg.grace).as_millis()
}

/// `n` values of a strategy from a fixed seed, as the Scala suites sample with `Seed(42L)`: the same values on every
/// run, so a test that checks coverage cannot flake.
pub fn sample<S: Strategy>(strategy: S, n: usize) -> Vec<S::Value> {
    let mut runner = TestRunner::deterministic();
    (0..n).map(|_| strategy.new_tree(&mut runner).expect("a value").current()).collect()
}

/// Port of `Gens`.
pub mod gens {
    use ledger_core::*;
    use proptest::prelude::*;

    pub fn status() -> impl Strategy<Value = TbStatus> + Clone {
        proptest::sample::select(TbStatus::ALL)
    }

    pub fn body() -> impl Strategy<Value = String> + Clone {
        "[a-zA-Z0-9]{0,12}"
    }

    pub fn key() -> impl Strategy<Value = ClientKey> + Clone {
        (ClientKey::MIN_TIME..=ClientKey::MAX_TIME, body()).prop_map(|(t, b)| ClientKey::new(WallTime::new(t), b))
    }

    pub fn capture_amount() -> impl Strategy<Value = CaptureAmount> + Clone {
        prop_oneof![Just(CaptureAmount::Full), (1i64..=1000).prop_map(CaptureAmount::partial)]
    }

    pub fn decline_reason() -> impl Strategy<Value = DeclineReason> + Clone {
        prop_oneof![
            Just(DeclineReason::Unknown),
            Just(DeclineReason::NotExecuted),
            status().prop_map(DeclineReason::KeyReused),
            status().prop_map(DeclineReason::Ledger),
        ]
    }

    pub fn capture_refusal() -> impl Strategy<Value = CaptureRefusal> + Clone {
        prop_oneof![
            Just(CaptureRefusal::NotApproved),
            Just(CaptureRefusal::Cancelled),
            Just(CaptureRefusal::Expired),
            Just(CaptureRefusal::ExceedsHold),
            status().prop_map(CaptureRefusal::KeyReused),
            status().prop_map(CaptureRefusal::Ledger),
        ]
    }

    pub fn outcome() -> impl Strategy<Value = Outcome> + Clone {
        prop_oneof![
            Just(Outcome::Auth(AuthOutcome::Approved)),
            decline_reason().prop_map(|r| Outcome::Auth(AuthOutcome::Declined(r))),
            Just(Outcome::Cancel(CancelOutcome::Cancelled)),
            Just(Outcome::Cancel(CancelOutcome::CancelRefused)),
            capture_amount().prop_map(|a| Outcome::Capture(CaptureOutcome::Captured(a))),
            capture_refusal().prop_map(|r| Outcome::Capture(CaptureOutcome::CaptureRefused(r))),
        ]
    }
}
