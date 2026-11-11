//! Port of `IdsSuite` and `PartitionerSuite`.

mod common;

use common::*;
use ledger_core::*;
use ledger_core::{ids, partitioner, record_codec};
use proptest::prelude::*;

// Fixed test vectors, the Scala core's own, computed independently of either core from the layout in README section
// 7. The layout is permanent: if one of these changes, cancels of existing holds would aim at the wrong ID. Never
// update them to make a test pass.
fn vector_key() -> ClientKey {
    ClientKey::new(WallTime::new(1_700_000_000_000), "card-auth-0001")
}
const VECTOR_A: &str = "018bcfe56800cb5ef6d202be8e8e0a72";
const VECTOR_V: &str = "018bcfe56800609fd3bb5fee3a9d3819";
const VECTOR_P: &str = "018bcfe56800ba5fd5428ff934943ba2";

#[test]
fn a_for_the_fixed_key_matches_its_test_vector() {
    assert_eq!(ids::authorization(&vector_key()).hex(), VECTOR_A);
}

#[test]
fn v_and_p_for_the_fixed_key_match_their_test_vectors() {
    let a = ids::authorization(&vector_key());
    assert_eq!(ids::void(a).hex(), VECTOR_V);
    assert_eq!(ids::post(a).hex(), VECTOR_P);
}

#[test]
fn the_hash_input_is_the_length_prefixed_tag_then_each_length_prefixed_field() {
    let encoded = ids::encode("ab", &[&[7], &[]]);
    assert_eq!(encoded, vec![0, 0, 0, 2, b'a', b'b', 0, 0, 0, 1, 7, 0, 0, 0, 0]);
}

#[test]
fn a_keys_time_must_keep_ids_away_from_0_and_the_maximum_value() {
    assert!(ClientKey::try_new(WallTime::new(0), "k").is_err());
    assert!(ClientKey::try_new(WallTime::new(ClientKey::MAX_TIME + 1), "k").is_err());
    assert!(ClientKey::try_new(WallTime::new(ClientKey::MIN_TIME), "k").is_ok());
    assert!(ClientKey::try_new(WallTime::new(ClientKey::MAX_TIME), "k").is_ok());
}

#[test]
fn a_keys_body_must_fit_what_the_command_log_can_write_for_it() {
    let t = WallTime::new(1_700_000_000_000);
    let longest = ClientKey::try_new(t, "x".repeat(ClientKey::MAX_BODY_BYTES));
    assert!(longest.is_ok());
    let _ = record_codec::encode(&Record::Command(Command::Cancel(longest.unwrap())));
    assert!(ClientKey::try_new(t, "x".repeat(ClientKey::MAX_BODY_BYTES + 1)).is_err());
    // NUL takes two bytes and every other character here three, as `writeUTF` writes them.
    assert!(ClientKey::try_new(t, "\0".repeat(ClientKey::MAX_BODY_BYTES / 2 + 1)).is_err());
    assert!(ClientKey::try_new(t, "€".repeat(ClientKey::MAX_BODY_BYTES / 3 + 1)).is_err());
    assert!(ClientKey::try_new(t, "€".repeat(ClientKey::MAX_BODY_BYTES / 3)).is_ok());
    // A character outside the Basic Multilingual Plane is two UTF-16 units, three bytes each.
    assert!(ClientKey::try_new(t, "😀".repeat(ClientKey::MAX_BODY_BYTES / 6 + 1)).is_err());
}

fn earlier_and_later() -> impl Strategy<Value = (ClientKey, ClientKey)> {
    (ClientKey::MIN_TIME..ClientKey::MAX_TIME)
        .prop_flat_map(|t1| (Just(t1), t1 + 1..=ClientKey::MAX_TIME, gens::body(), gens::body()))
        .prop_map(|(t1, t2, b1, b2)| (ClientKey::new(WallTime::new(t1), b1), ClientKey::new(WallTime::new(t2), b2)))
}

proptest! {
    #[test]
    fn a_takes_the_keys_time_in_its_high_48_bits_v_and_p_take_as_time(k in gens::key()) {
        let a = ids::authorization(&k);
        prop_assert_eq!(ids::time_of(a), k.created_at());
        prop_assert_eq!(ids::time_of(ids::void(a)), k.created_at());
        prop_assert_eq!(ids::time_of(ids::post(a)), k.created_at());
    }

    #[test]
    fn no_id_is_0_or_the_maximum_value(k in gens::key()) {
        let a = ids::authorization(&k);
        for id in [a, ids::void(a), ids::post(a)] {
            prop_assert!(id.value() != 0 && id.value() != u128::MAX);
        }
    }

    #[test]
    fn the_encodings_of_different_id_kinds_never_coincide(k in gens::key(), other in gens::key()) {
        let a = ids::authorization(&other);
        let auth = ids::encode("authorization", &[&k.created_at().millis().to_be_bytes(), k.body().as_bytes()]);
        let void = ids::encode("void", &[&a.value().to_be_bytes()]);
        let post = ids::encode("post", &[&a.value().to_be_bytes()]);
        prop_assert!(auth != void && auth != post && void != post);
    }

    #[test]
    fn keys_with_later_times_give_higher_ids((earlier, later) in earlier_and_later()) {
        prop_assert!(ids::authorization(&earlier) < ids::authorization(&later));
    }

    #[test]
    fn the_same_key_always_gives_the_same_ids(k in gens::key()) {
        let copy = ClientKey::new(k.created_at(), k.body());
        prop_assert_eq!(ids::authorization(&k), ids::authorization(&copy));
        prop_assert_eq!(ids::void(ids::authorization(&k)), ids::void(ids::authorization(&copy)));
    }
}

// Kafka's own vectors for murmur2, from `UtilsTest.testMurmur2` in the Kafka clients.
#[test]
fn murmur2_matches_kafkas_test_vectors() {
    let cases = [
        ("21", -973_932_308),
        ("foobar", -790_332_482),
        ("a-little-bit-long-string", -985_981_536),
        ("a-little-bit-longer-string", -1_486_304_829),
        ("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58_897_971),
        ("abc", 479_470_107),
    ];
    for (input, expected) in cases {
        assert_eq!(partitioner::murmur2(input.as_bytes()), expected, "{input}");
    }
}

#[test]
fn every_command_about_a_hold_lands_on_the_holds_partition() {
    let k = key(1);
    let partitions: Vec<Partition> =
        [auth_command(&k), cancel_command(&k), capture_command(&k)].iter().map(|c| partitioner::partition(c.a(), 64)).collect();
    assert!(partitions.windows(2).all(|w| w[0] == w[1]));
}
