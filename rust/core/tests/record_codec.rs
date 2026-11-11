//! Port of `RecordCodecSuite`, with two checks of the string encoding that the Scala suite gets from the JDK.

mod common;

use common::*;
use ledger_core::record_codec;
use ledger_core::*;
use proptest::prelude::*;

fn command() -> impl Strategy<Value = Command> {
    gens::key().prop_flat_map(|key| {
        let (k1, k2, k3) = (key.clone(), key.clone(), key);
        prop_oneof![
            (any::<u128>(), any::<u128>(), any::<u128>(), 1..=i32::MAX, 1..=i32::MAX).prop_map(move |(d, c, amount, ledger, hold)| {
                Command::Authorize(AuthorizeCommand {
                    key: k1.clone(),
                    debit: AccountId::new(d),
                    credit: AccountId::new(c),
                    amount,
                    ledger,
                    hold_timeout_seconds: hold,
                })
            }),
            Just(Command::Cancel(k2)),
            gens::capture_amount().prop_map(move |amount| Command::Capture(k3.clone(), amount)),
        ]
    })
}

fn record() -> impl Strategy<Value = Record> {
    prop_oneof![
        command().prop_map(Record::Command),
        (command(), gens::outcome(), prop_oneof![Just(Writer::Edge), Just(Writer::Resolver)])
            .prop_map(|(c, o, w)| Record::Outcome(c, o, w)),
    ]
}

proptest! {
    #[test]
    fn a_record_reads_back_as_it_was_written(r in record()) {
        prop_assert_eq!(record_codec::decode(&record_codec::encode(&r)), Ok(r));
    }

    #[test]
    fn every_prefix_of_a_record_is_an_error_never_a_different_record(r in record()) {
        let bytes = record_codec::encode(&r);
        for n in 0..bytes.len() {
            prop_assert!(record_codec::decode(&bytes[..n]).is_err());
        }
    }

    /// Beyond the Scala suite: any Unicode body, including NUL and characters outside the Basic Multilingual Plane,
    /// which modified UTF-8 writes in forms plain UTF-8 does not.
    #[test]
    fn any_unicode_body_reads_back(t in ClientKey::MIN_TIME..=ClientKey::MAX_TIME, body in any::<String>()) {
        let r = Record::Command(Command::Cancel(ClientKey::new(WallTime::new(t), body)));
        prop_assert_eq!(record_codec::decode(&record_codec::encode(&r)), Ok(r));
    }
}

#[test]
fn an_unknown_version_tag_or_status_is_an_error_since_the_resolver_must_not_read_past_it() {
    let bytes = record_codec::encode(&Record::Command(Command::Cancel(ClientKey::new(WallTime::new(5), "k"))));
    let mut wrong_version = bytes.clone();
    wrong_version[0] = 9;
    assert!(record_codec::decode(&wrong_version).unwrap_err().contains("version"));
    let mut wrong_tag = bytes.clone();
    wrong_tag[1] = 9;
    assert!(record_codec::decode(&wrong_tag).is_err());
    let mut trailing = bytes;
    trailing.push(0);
    assert!(record_codec::decode(&trailing).unwrap_err().contains("after the record"));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// The log keeps records for 30 days across deployments, so a format change must be deliberate. The Scala suite's own
// vector: either core reads what the other wrote.
#[test]
fn the_format_does_not_change_by_accident() {
    let r = Record::Outcome(
        Command::Capture(ClientKey::new(WallTime::new(1), "k"), CaptureAmount::partial(2)),
        Outcome::Capture(CaptureOutcome::CaptureRefused(CaptureRefusal::Ledger(TbStatus::Exists))),
        Writer::Resolver,
    );
    let expected = String::from("01010200000000000000010001") + "6b" + "010000000000000002" + "0505" + "0006" + "457869737473" + "01";
    assert_eq!(hex(&record_codec::encode(&r)), expected);
}

// The bytes Java's `DataOutputStream.writeUTF` wrote for "a", NUL, "é", "€" and "😀" on JDK 25: NUL as two bytes, and
// the emoji as two 3-byte surrogates.
#[test]
fn a_body_is_written_in_javas_modified_utf8() {
    let r = Record::Command(Command::Cancel(ClientKey::new(WallTime::new(1), "a\u{0}é€😀")));
    let bytes = record_codec::encode(&r);
    assert_eq!(hex(&bytes[11..]), "000e61c080c3a9e282aceda0bdedb880");
    assert_eq!(record_codec::decode(&bytes), Ok(r));
}
