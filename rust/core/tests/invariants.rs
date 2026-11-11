//! Port of `InvariantsSuite` and `LivenessSuite`, over histories written by hand. History times are milliseconds;
//! TigerBeetle timestamps are nanoseconds.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::*;
use ledger_core::tb::transfers;
use ledger_core::*;
use ledger_core::{invariants, liveness};

/// A history written by hand, one record at a time.
#[derive(Clone)]
struct HistoryBuilder {
    caller: Vec<CallerRecord>,
    log: Vec<LogRecord>,
    edges: Vec<EdgeRecord>,
    transfers: Vec<LedgerTransfer>,
    accounts: Vec<LedgerAccount>,
    expiries: Option<BTreeMap<TransferId, i64>>,
    scanned_at: i64,
    healed_at: Option<i64>,
    faults_stopped_at: Option<i64>,
    ended_at: i64,
}

impl Default for HistoryBuilder {
    fn default() -> Self {
        HistoryBuilder {
            caller: vec![],
            log: vec![],
            edges: vec![],
            transfers: vec![],
            accounts: vec![],
            expiries: None,
            scanned_at: 1_000_000_000,
            healed_at: None,
            faults_stopped_at: None,
            ended_at: 100_000,
        }
    }
}

impl HistoryBuilder {
    fn sent(mut self, at: i64, r: &Request) -> Self {
        self.caller.push(CallerRecord::Sent(HistoryTime::new(at), r.clone()));
        self
    }

    fn received(mut self, at: i64, r: &Request, reply: Reply) -> Self {
        self.caller.push(CallerRecord::Received(HistoryTime::new(at), r.clone(), reply));
        self
    }

    /// A request at an edge instance, arriving at `arrived` and answered at `answered`, both by the edge's clock.
    fn edge(mut self, at: i64, r: &Request, arrived: i64, answered: Option<i64>) -> Self {
        let answer = answered.map(|t| (EdgeTime::new(t), Reply::Decided(APPROVED)));
        self.edges.push(EdgeRecord {
            instance: 1,
            at: HistoryTime::new(at),
            request: r.clone(),
            arrived: EdgeTime::new(arrived),
            answered: answer,
        });
        self
    }

    fn logged(self, at: i64, c: &Command) -> Self {
        self.append(at, Record::Command(c.clone()))
    }

    fn decided(self, at: i64, c: &Command, o: Outcome) -> Self {
        self.append(at, Record::Outcome(c.clone(), o, Writer::Edge))
    }

    fn decided_by(self, at: i64, c: &Command, o: Outcome, by: Writer) -> Self {
        self.append(at, Record::Outcome(c.clone(), o, by))
    }

    fn ledger(mut self, t: Transfer, timestamp: i64) -> Self {
        self.transfers.push(LedgerTransfer { cluster: Period::new(0), transfer: t, timestamp });
        self
    }

    fn limited(mut self, id: AccountId) -> Self {
        self.accounts.push(LedgerAccount {
            cluster: Period::new(0),
            id,
            debits_must_not_exceed_credits: true,
            credits_must_not_exceed_debits: false,
        });
        self
    }

    fn history(&self) -> History {
        let snapshot = LedgerSnapshot::new(
            self.transfers.clone(),
            self.accounts.clone(),
            BTreeMap::from([(Period::new(0), self.scanned_at)]),
            self.expiries.clone(),
            self.healed_at.map(|t| (Period::new(0), t)).into_iter().collect(),
        );
        History {
            caller: self.caller.clone(),
            log: self.log.clone(),
            edges: self.edges.clone(),
            ledger: snapshot,
            faults_stopped_at: self.faults_stopped_at.map(HistoryTime::new),
            ended_at: HistoryTime::new(self.ended_at),
            log_clock_slack: Span::ZERO,
        }
    }

    fn append(mut self, at: i64, r: Record) -> Self {
        let offset = Offset::new(self.log.len() as i64);
        self.log.push(LogRecord { partition: Partition::new(0), offset, at: HistoryTime::new(at), record: r });
        self
    }
}

const APPROVED: Outcome = Outcome::Auth(AuthOutcome::Approved);
const CANCELLED: Outcome = Outcome::Cancel(CancelOutcome::Cancelled);

fn declined(r: DeclineReason) -> Outcome {
    Outcome::Auth(AuthOutcome::Declined(r))
}

fn captured(amount: CaptureAmount) -> Outcome {
    Outcome::Capture(CaptureOutcome::Captured(amount))
}

/// Port of `Histories`.
fn k() -> ClientKey {
    key(1)
}

fn auth() -> Request {
    authorize(&k())
}

fn auth_c() -> Command {
    auth_command(&k())
}

fn a() -> TransferId {
    auth_c().a()
}

fn cancel() -> Request {
    Request::Cancel(k())
}

fn cancel_c() -> Command {
    cancel_command(&k())
}

fn hold() -> Transfer {
    transfers::authorization(&authorize_command(&k()), &LEDGER_CFG)
}

/// A capture as TigerBeetle records it: the hold's accounts, ledger and code, and the amount moved.
fn posted() -> Transfer {
    let h = hold();
    Transfer {
        debit: h.debit,
        credit: h.credit,
        amount: h.amount,
        ledger: h.ledger,
        code: h.code,
        ..transfers::post(a(), CaptureAmount::Full)
    }
}

/// Authorized, logged, approved by the ledger, logged and answered.
fn approved() -> HistoryBuilder {
    HistoryBuilder::default().sent(0, &auth()).logged(1, &auth_c()).ledger(hold(), 10).decided(20, &auth_c(), APPROVED).received(
        30,
        &auth(),
        Reply::Decided(APPROVED),
    )
}

fn broken(h: &HistoryBuilder) -> BTreeSet<&'static str> {
    invariants::check(&h.history()).into_iter().map(|v| v.property).collect()
}

#[track_caller]
fn assert_holds(h: &HistoryBuilder) {
    assert_eq!(invariants::check(&h.history()), vec![]);
}

#[test]
fn an_approved_logged_and_answered_authorization_breaks_nothing() {
    assert_holds(&approved());
}

#[test]
fn no_transfer_is_applied_twice() {
    assert!(broken(&approved().ledger(hold(), 11)).contains(invariants::NO_TRANSFER_TWICE));
    let posted_and_voided = approved().ledger(posted(), 40).ledger(transfers::void(a()), 50);
    assert!(broken(&posted_and_voided).contains(invariants::NO_TRANSFER_TWICE));
}

#[test]
fn one_outcome_an_approval_and_a_decline_for_one_authorization() {
    let h = approved().decided_by(40, &auth_c(), declined(DeclineReason::NotExecuted), Writer::Resolver);
    assert!(broken(&h).contains(invariants::ONE_OUTCOME));
}

#[test]
fn a_declined_authorization_then_a_fenced_duplicate_reported_not_executed_passes() {
    let rejected = declined(DeclineReason::Ledger(TbStatus::AccountsMustBeDifferent));
    let h = HistoryBuilder::default()
        .sent(0, &auth())
        .logged(1, &auth_c())
        .decided(10, &auth_c(), rejected)
        .received(11, &auth(), Reply::Decided(rejected))
        .logged(20, &cancel_c())
        .ledger(transfers::fence(a(), &LEDGER_CFG), 30)
        .decided(40, &cancel_c(), CANCELLED)
        .logged(50, &auth_c())
        .decided_by(60, &auth_c(), declined(DeclineReason::NotExecuted), Writer::Resolver);
    assert_holds(&h);
}

#[test]
fn the_resolver_repeating_an_edges_decline_with_a_generic_reason_passes() {
    let specific = declined(DeclineReason::Ledger(TbStatus::ExceedsCredits));
    let h = HistoryBuilder::default()
        .sent(0, &auth())
        .logged(1, &auth_c())
        .decided(10, &auth_c(), specific)
        .received(11, &auth(), Reply::Decided(specific))
        .decided_by(5000, &auth_c(), declined(DeclineReason::Unknown), Writer::Resolver);
    assert_holds(&h);
}

fn answered_early() -> HistoryBuilder {
    HistoryBuilder::default().sent(0, &auth()).logged(1, &auth_c()).ledger(hold(), 10).received(15, &auth(), Reply::Decided(APPROVED))
}

#[test]
fn logged_before_answered_an_answer_before_its_outcome() {
    let h = answered_early().decided(20, &auth_c(), APPROVED);
    assert_eq!(broken(&h), BTreeSet::from([invariants::LOGGED_BEFORE_ANSWERED]));
}

#[test]
fn logged_before_answered_a_real_history_allows_for_the_logs_clock_and_for_no_more_than_it_states() {
    let h = answered_early().decided(20, &auth_c(), APPROVED).history();
    let found = |slack: Span| -> Vec<&'static str> {
        invariants::answered_after_logged(&History { log_clock_slack: slack, ..h.clone() }).into_iter().map(|v| v.property).collect()
    };
    assert_eq!(found(Span::from_millis(5)), Vec::<&str>::new());
    assert_eq!(found(Span::from_millis(4)), vec![invariants::LOGGED_BEFORE_ANSWERED]);
    // An outcome that never landed is no matter of clocks, however much is allowed.
    let never = History { log_clock_slack: Span::from_hours(1), ..answered_early().history() };
    let properties: Vec<&str> = invariants::answered_after_logged(&never).into_iter().map(|v| v.property).collect();
    assert_eq!(properties, vec![invariants::LOGGED_BEFORE_ANSWERED]);
}

#[test]
fn refusal_leaves_no_hold_a_hidden_resend_creates_the_hold_while_the_caller_hears_a_refusal() {
    let h = HistoryBuilder::default().sent(0, &auth()).logged(1, &auth_c()).ledger(hold(), 10).received(
        20,
        &auth(),
        Reply::Refused(RefusalReason::AcknowledgedTooLate),
    );
    assert!(broken(&h).contains(invariants::REFUSAL_LEAVES_NO_HOLD));
}

#[test]
fn a_refused_authorization_resent_with_the_same_key_and_approved_passes() {
    let h = HistoryBuilder::default()
        .sent(0, &auth())
        .received(5, &auth(), Reply::Refused(RefusalReason::Overload))
        .sent(10, &auth())
        .logged(11, &auth_c())
        .ledger(hold(), 20)
        .decided(30, &auth_c(), APPROVED)
        .received(40, &auth(), Reply::Decided(APPROVED));
    assert_holds(&h);
}

fn cancelled_after_approval() -> HistoryBuilder {
    approved()
        .sent(100, &cancel())
        .logged(101, &cancel_c())
        .decided(120, &cancel_c(), CANCELLED)
        .received(130, &cancel(), Reply::Decided(CANCELLED))
        .decided_by(140, &auth_c(), APPROVED, Writer::Resolver)
}

#[test]
fn cancel_leaves_no_hold_an_approval_after_the_cancel_and_the_hold_survives() {
    assert_eq!(broken(&cancelled_after_approval()), BTreeSet::from([invariants::CANCEL_LEAVES_NO_HOLD]));
}

#[test]
fn an_approval_after_the_cancel_with_the_hold_voided_passes_the_merge_keeps_the_cancel() {
    assert_holds(&cancelled_after_approval().ledger(transfers::void(a()), 110));
}

#[test]
fn a_hold_reported_cancelled_that_had_already_expired_passes() {
    let expired = HistoryBuilder { scanned_at: 10 + i64::from(HOLD_TIMEOUT) * 1_000_000_000, ..cancelled_after_approval() };
    assert_holds(&expired);
}

#[test]
fn cancel_leaves_no_hold_after_a_cancel_is_reported_cancelled_a_capture_succeeds() {
    let capture = Request::Capture(k(), CaptureAmount::Full);
    let h = cancelled_after_approval().ledger(posted(), 200).sent(190, &capture).logged(191, &capture_command(&k())).decided(
        210,
        &capture_command(&k()),
        captured(CaptureAmount::Full),
    );
    assert!(broken(&h).contains(invariants::CANCEL_LEAVES_NO_HOLD));
    assert!(broken(&h).contains(invariants::ONE_OUTCOME));
}

fn captured_in_full() -> HistoryBuilder {
    approved().logged(100, &capture_command(&k())).ledger(posted(), 110).decided(120, &capture_command(&k()), captured(CaptureAmount::Full))
}

#[test]
fn a_hold_captured_once_in_full_passes() {
    assert_holds(&captured_in_full());
}

#[test]
fn capture_once_captured_twice() {
    let again = Transfer { id: ids::void(a()), ..posted() };
    assert!(broken(&captured_in_full().ledger(again, 130)).contains(invariants::CAPTURE_ONCE));
}

#[test]
fn matches_ledger_captured_in_full_but_the_ledger_moved_less() {
    let less = Transfer { amount: hold().amount - 1, ..posted() };
    let h = approved().logged(100, &capture_command(&k())).ledger(less, 110).decided(
        120,
        &capture_command(&k()),
        captured(CaptureAmount::Full),
    );
    assert_eq!(broken(&h), BTreeSet::from([invariants::MATCHES_LEDGER]));
}

#[test]
fn matches_ledger_a_full_capture_beside_a_partial_one_says_the_hold_was_all_moved() {
    let whole = CaptureAmount::partial(i64::try_from(hold().amount).unwrap());
    let all = approved()
        .logged(100, &capture_command(&k()))
        .ledger(posted(), 110)
        .decided(120, &capture_command(&k()), captured(CaptureAmount::Full))
        .decided(121, &capture_command(&k()), captured(whole));
    assert_holds(&all);
    let less = approved()
        .logged(100, &capture_command(&k()))
        .ledger(Transfer { amount: 1, ..posted() }, 110)
        .decided(120, &capture_command(&k()), captured(CaptureAmount::Full))
        .decided(121, &capture_command(&k()), captured(CaptureAmount::partial(1)));
    assert_eq!(broken(&less), BTreeSet::from([invariants::MATCHES_LEDGER]));
}

#[test]
fn capture_once_captured_for_more_than_it_held() {
    let h = approved().ledger(transfers::post(a(), CaptureAmount::partial(1000)), 110);
    assert!(broken(&h).contains(invariants::CAPTURE_ONCE));
}

#[test]
fn once_captured_a_hold_is_never_reported_cancelled() {
    let h = captured_in_full().decided(130, &cancel_c(), CANCELLED);
    assert!(broken(&h).contains(invariants::ONE_OUTCOME));
    assert!(broken(&h).contains(invariants::CANCEL_LEAVES_NO_HOLD));
}

#[test]
fn matches_ledger_approved_but_the_ledger_has_no_hold() {
    let h = HistoryBuilder::default().sent(0, &auth()).logged(1, &auth_c()).decided(20, &auth_c(), APPROVED).received(
        30,
        &auth(),
        Reply::Decided(APPROVED),
    );
    assert_eq!(broken(&h), BTreeSet::from([invariants::MATCHES_LEDGER]));
}

const FUNDER: AccountId = AccountId::of(500);

fn fund(amount: u128) -> Transfer {
    Transfer {
        id: ids::authorization(&key(99)),
        debit: FUNDER,
        credit: CUSTOMER,
        amount,
        pending_id: TransferId::ZERO,
        ledger: 1,
        code: 2,
        flags: TransferFlags::NONE,
        timeout_seconds: 0,
    }
}

#[test]
fn balances_within_limits_a_hold_beyond_the_customers_credits() {
    assert_eq!(broken(&approved().limited(CUSTOMER)), BTreeSet::from([invariants::BALANCES_WITHIN_LIMITS]));
}

#[test]
fn a_hold_within_the_customers_credits_passes() {
    assert_holds(&approved().limited(CUSTOMER).ledger(fund(100), 5));
}

#[test]
fn approval_stands() {
    let broken_keeping_contracts = |h: &HistoryBuilder| -> BTreeSet<&'static str> {
        invariants::check_kept_contracts(&h.history()).into_iter().map(|v| v.property).collect()
    };
    let voided = approved().logged(30, &cancel_c()).ledger(transfers::void(a()), 40);
    assert!(broken_keeping_contracts(&voided).contains(invariants::APPROVAL_STANDS));
    assert!(!broken_keeping_contracts(&voided.clone().sent(25, &cancel())).contains(invariants::APPROVAL_STANDS));
    // A caller that sent a key twice can hear this about a copy of its own that timed out, and the money stays safe.
    assert!(!broken(&voided).contains(invariants::APPROVAL_STANDS));
}

#[test]
fn writes_follow_commands() {
    assert!(broken(&HistoryBuilder::default().ledger(hold(), 10)).contains(invariants::WRITES_FOLLOW_COMMANDS));
    assert!(broken(&approved().ledger(transfers::void(a()), 40)).contains(invariants::WRITES_FOLLOW_COMMANDS));
    assert!(broken(&approved().ledger(posted(), 40)).contains(invariants::WRITES_FOLLOW_COMMANDS));
    assert!(!broken(&approved().logged(30, &cancel_c()).ledger(transfers::void(a()), 40)).contains(invariants::WRITES_FOLLOW_COMMANDS));
    assert!(!broken(&captured_in_full()).contains(invariants::WRITES_FOLLOW_COMMANDS));
}

#[test]
fn an_expired_hold_releases_its_amount_before_the_next_one() {
    let second = transfers::authorization(&authorize_command(&key(2)), &LEDGER_CFG);
    let after_expiry = i64::from(HOLD_TIMEOUT) * 1_000_000_000 + 20;
    let logged = HistoryBuilder::default().logged(0, &auth_c()).logged(0, &auth_command_amount(&key(2), 100));
    let base = logged.limited(CUSTOMER).ledger(fund(150), 1).ledger(hold(), 10);
    assert_holds(&HistoryBuilder { scanned_at: after_expiry + 1, ..base.clone().ledger(second, after_expiry) });
    assert!(broken(&base.ledger(second, 20)).contains(invariants::BALANCES_WITHIN_LIMITS));
}

// LivenessSuite

const BOUNDS: liveness::Bounds = liveness::Bounds {
    deadline: Span::from_millis(1500),
    answer_within: Span::from_secs(2),
    outcome_within: Span::from_secs(10),
    settle: Span::from_secs(1),
    expiry_lag: Span::from_secs(1),
};

fn late(h: &HistoryBuilder) -> BTreeSet<&'static str> {
    liveness::check(&h.history(), &BOUNDS).into_iter().map(|v| v.property).collect()
}

fn none() -> BTreeSet<&'static str> {
    BTreeSet::new()
}

#[test]
fn liveness_an_approved_logged_and_answered_authorization_breaks_nothing() {
    assert_eq!(liveness::check(&approved().history(), &BOUNDS), vec![]);
}

#[test]
fn answered_no_answer_after_faults_stopped() {
    let h = HistoryBuilder { faults_stopped_at: Some(1000), ..HistoryBuilder::default() }.sent(5000, &auth());
    assert_eq!(late(&h), BTreeSet::from([liveness::ANSWERED]));
}

#[test]
fn answered_by_the_edges_clock_answered_after_the_deadline() {
    // On time by the history's clock, late by the edge's own.
    let h = HistoryBuilder::default().edge(5000, &auth(), 70_000, Some(71_600));
    assert_eq!(late(&h), BTreeSet::from([liveness::ANSWERED]));
}

#[test]
fn answered_by_the_edges_clock_never_answered_by_a_live_instance() {
    assert_eq!(late(&HistoryBuilder::default().edge(5000, &auth(), 70_000, None)), BTreeSet::from([liveness::ANSWERED]));
}

#[test]
fn answered_within_the_deadline_by_the_edges_clock_passes_and_then_the_callers_side_is_not_used() {
    let h = HistoryBuilder::default().edge(5000, &auth(), 70_000, Some(71_400)).sent(5000, &auth());
    assert_eq!(late(&h), none());
}

#[test]
fn requests_sent_during_faults_or_too_close_to_the_end_are_not_judged() {
    assert_eq!(late(&HistoryBuilder { faults_stopped_at: Some(10_000), ..HistoryBuilder::default() }.sent(5000, &auth())), none());
    assert_eq!(late(&HistoryBuilder::default().sent(99_500, &auth())), none());
}

#[test]
fn command_has_outcome() {
    assert_eq!(late(&HistoryBuilder::default().logged(1, &auth_c())), BTreeSet::from([liveness::COMMAND_HAS_OUTCOME]));
    assert_eq!(late(&HistoryBuilder::default().logged(95_000, &auth_c())), none());
}

#[test]
fn cancel_acknowledged() {
    let h = HistoryBuilder::default().sent(0, &cancel()).received(10, &cancel(), Reply::Retry);
    assert_eq!(late(&h), BTreeSet::from([liveness::CANCEL_ACKNOWLEDGED]));
    assert_eq!(late(&h.sent(20, &cancel()).received(30, &cancel(), Reply::Decided(CANCELLED))), none());
}

#[test]
fn hold_ends() {
    let due = 10 + i64::from(HOLD_TIMEOUT) * 1_000_000_000 + 1_000_000_000;
    let held = HistoryBuilder { scanned_at: due + 1, expiries: Some(BTreeMap::new()), ..HistoryBuilder::default().ledger(hold(), 10) };
    assert_eq!(late(&held), BTreeSet::from([liveness::HOLD_ENDS]));
    assert_eq!(late(&HistoryBuilder { expiries: Some(BTreeMap::from([(a(), due - 5)])), ..held }), none());
    let no_timeout = Transfer { timeout_seconds: 0, ..hold() };
    assert_eq!(late(&HistoryBuilder::default().ledger(no_timeout, 10)), BTreeSet::from([liveness::HOLD_ENDS]));
}

#[test]
fn a_hold_that_came_due_while_faults_ran_has_until_they_stop_plus_the_lag() {
    let healed = 10 + i64::from(HOLD_TIMEOUT) * 1_000_000_000 + 5_000_000_000;
    let late_but_fine = HistoryBuilder {
        scanned_at: healed * 2,
        healed_at: Some(healed),
        expiries: Some(BTreeMap::from([(a(), healed + 1)])),
        ..HistoryBuilder::default().ledger(hold(), 10)
    };
    assert_eq!(late(&late_but_fine), none());
    let too_late = HistoryBuilder { expiries: Some(BTreeMap::from([(a(), healed + 1_000_000_001)])), ..late_but_fine.clone() };
    assert_eq!(late(&too_late), BTreeSet::from([liveness::HOLD_ENDS]));
    assert_eq!(late(&HistoryBuilder { expiries: Some(BTreeMap::new()), ..late_but_fine }), BTreeSet::from([liveness::HOLD_ENDS]));
}

#[test]
fn refusals_stop() {
    let h = HistoryBuilder::default()
        .received(100, &auth(), Reply::Refused(RefusalReason::Overload))
        .received(200, &auth(), Reply::Refused(RefusalReason::NoLeader))
        .history();
    assert_eq!(liveness::refusals_stop(&h, HistoryTime::new(150)), vec![]);
    let properties: Vec<&str> = liveness::refusals_stop(&h, HistoryTime::new(50)).into_iter().map(|v| v.property).collect();
    assert_eq!(properties, vec![liveness::REFUSALS_STOP]);
}

#[test]
fn throughput_floor() {
    let h = (0..10).fold(HistoryBuilder::default(), |b, i| b.received(i * 100, &auth(), Reply::Decided(APPROVED))).history();
    let window = Span::from_millis(500);
    assert_eq!(liveness::throughput_floor(&h, HistoryTime::new(0), HistoryTime::new(1000), window, 5), vec![]);
    assert_eq!(liveness::throughput_floor(&h, HistoryTime::new(0), HistoryTime::new(2000), window, 5).len(), 2);
}
