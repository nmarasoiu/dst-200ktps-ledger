//! A record of the command log as bytes: the edge and the resolver write it, and the resolver and the offline checker
//! read it for as long as the log keeps it (README section 4). Fields are written in a fixed order after a version
//! byte. A TigerBeetle status goes by name, since its numeric code is TigerBeetle's and its position here is not
//! stable. Port of `RecordCodec.scala`, byte for byte: integers are big-endian as Java's `DataOutputStream` writes
//! them, and strings are Java's modified UTF-8 behind a 2-byte length, as `writeUTF` writes them, so either core
//! reads what the other wrote.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::domain::{
    AccountId, AuthOutcome, AuthorizeCommand, CancelOutcome, CaptureAmount, CaptureOutcome, CaptureRefusal, Command, DeclineReason,
    Outcome, Record, Writer,
};
use crate::ids::ClientKey;
use crate::machine::WallTime;
use crate::tb::TbStatus;

const VERSION: u8 = 1;

/// `ClientKey` holds every body within `writeUTF`'s limit, so this never panics.
pub fn encode(record: &Record) -> Vec<u8> {
    let mut out = Vec::with_capacity(96);
    out.push(VERSION);
    match record {
        Record::Command(command) => {
            out.push(0);
            write_command(&mut out, command);
        }
        Record::Outcome(command, outcome, writer) => {
            out.push(1);
            write_command(&mut out, command);
            write_outcome(&mut out, outcome);
            out.push(writer.ordinal());
        }
    }
    out
}

/// A record it cannot read is an error, never something to skip: the resolver must not pass a command unseen.
pub fn decode(bytes: &[u8]) -> Result<Record, String> {
    let mut input = Reader { bytes, at: 0 };
    let version = input.i8()?;
    if version != VERSION as i8 {
        return Err(format!("unknown record version {version}"));
    }
    let record = match input.i8()? {
        0 => Record::Command(read_command(&mut input)?),
        1 => {
            let command = read_command(&mut input)?;
            let outcome = read_outcome(&mut input)?;
            let ordinal = input.i8()?;
            let writer = u8::try_from(ordinal).ok().and_then(Writer::from_ordinal).ok_or_else(|| unknown("writer", ordinal))?;
            Record::Outcome(command, outcome, writer)
        }
        tag => return Err(unknown("record", tag)),
    };
    let rest = bytes.len() - input.at;
    if rest > 0 { Err(format!("{rest} bytes after the record")) } else { Ok(record) }
}

fn unknown(what: &str, tag: i8) -> String {
    format!("unreadable record: unknown {what} tag {tag}")
}

fn write_u128(out: &mut Vec<u8>, n: u128) {
    out.extend_from_slice(&n.to_be_bytes());
}

/// The bytes `writeUTF` takes for a string, its 2-byte length aside: one per UTF-16 unit from 1 to 127, two for 0 and
/// up to 2047, three for the rest.
pub(crate) fn utf_length(s: &str) -> usize {
    s.encode_utf16()
        .map(|unit| match unit {
            0x0001..=0x007f => 1,
            0x0000 | 0x0080..=0x07ff => 2,
            _ => 3,
        })
        .sum()
}

fn write_utf(out: &mut Vec<u8>, s: &str) {
    let mut bytes = Vec::with_capacity(s.len());
    for unit in s.encode_utf16() {
        match unit {
            0x0001..=0x007f => bytes.push(unit as u8),
            0x0000 | 0x0080..=0x07ff => {
                bytes.push(0xc0 | (unit >> 6) as u8);
                bytes.push(0x80 | (unit & 0x3f) as u8);
            }
            _ => {
                bytes.push(0xe0 | (unit >> 12) as u8);
                bytes.push(0x80 | ((unit >> 6) & 0x3f) as u8);
                bytes.push(0x80 | (unit & 0x3f) as u8);
            }
        }
    }
    let length = u16::try_from(bytes.len()).unwrap_or_else(|_| panic!("encoded string too long: {} bytes", bytes.len()));
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&bytes);
}

fn write_key(out: &mut Vec<u8>, key: &ClientKey) {
    out.extend_from_slice(&key.created_at().millis().to_be_bytes());
    write_utf(out, key.body());
}

fn write_amount(out: &mut Vec<u8>, amount: &CaptureAmount) {
    match amount {
        CaptureAmount::Full => out.push(0),
        CaptureAmount::Partial(n) => {
            out.push(1);
            out.extend_from_slice(&n.get().to_be_bytes());
        }
    }
}

fn write_command(out: &mut Vec<u8>, command: &Command) {
    match command {
        Command::Authorize(c) => {
            out.push(0);
            write_key(out, &c.key);
            write_u128(out, c.debit.value());
            write_u128(out, c.credit.value());
            write_u128(out, c.amount);
            out.extend_from_slice(&c.ledger.to_be_bytes());
            out.extend_from_slice(&c.hold_timeout_seconds.to_be_bytes());
        }
        Command::Cancel(key) => {
            out.push(1);
            write_key(out, key);
        }
        Command::Capture(key, amount) => {
            out.push(2);
            write_key(out, key);
            write_amount(out, amount);
        }
    }
}

fn write_status(out: &mut Vec<u8>, status: TbStatus) {
    write_utf(out, status.name());
}

fn write_outcome(out: &mut Vec<u8>, outcome: &Outcome) {
    match outcome {
        Outcome::Auth(AuthOutcome::Approved) => out.push(0),
        Outcome::Auth(AuthOutcome::Declined(reason)) => {
            out.push(1);
            match reason {
                DeclineReason::Unknown => out.push(0),
                DeclineReason::NotExecuted => out.push(1),
                DeclineReason::KeyReused(status) => {
                    out.push(2);
                    write_status(out, *status);
                }
                DeclineReason::Ledger(status) => {
                    out.push(3);
                    write_status(out, *status);
                }
            }
        }
        Outcome::Cancel(CancelOutcome::Cancelled) => out.push(2),
        Outcome::Cancel(CancelOutcome::CancelRefused) => out.push(3),
        Outcome::Capture(CaptureOutcome::Captured(amount)) => {
            out.push(4);
            write_amount(out, amount);
        }
        Outcome::Capture(CaptureOutcome::CaptureRefused(reason)) => {
            out.push(5);
            match reason {
                CaptureRefusal::NotApproved => out.push(0),
                CaptureRefusal::Cancelled => out.push(1),
                CaptureRefusal::Expired => out.push(2),
                CaptureRefusal::ExceedsHold => out.push(3),
                CaptureRefusal::KeyReused(status) => {
                    out.push(4);
                    write_status(out, *status);
                }
                CaptureRefusal::Ledger(status) => {
                    out.push(5);
                    write_status(out, *status);
                }
            }
        }
    }
}

/// The reading side of `DataInputStream`: running out of bytes is an error, as its `EOFException` is.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let end =
            self.at.checked_add(N).filter(|end| *end <= self.bytes.len()).ok_or_else(|| "unreadable record: end of input".to_string())?;
        let mut out = [0u8; N];
        out.copy_from_slice(&self.bytes[self.at..end]);
        self.at = end;
        Ok(out)
    }

    fn i8(&mut self) -> Result<i8, String> {
        Ok(i8::from_be_bytes(self.take::<1>()?))
    }

    fn i32(&mut self) -> Result<i32, String> {
        Ok(i32::from_be_bytes(self.take::<4>()?))
    }

    fn i64(&mut self) -> Result<i64, String> {
        Ok(i64::from_be_bytes(self.take::<8>()?))
    }

    fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_be_bytes(self.take::<16>()?))
    }

    /// `readUTF`: a 2-byte length, then modified UTF-8. Java would accept a lone surrogate into its string; a Rust
    /// string cannot hold one, so it is an error here.
    fn utf(&mut self) -> Result<String, String> {
        let length = u16::from_be_bytes(self.take::<2>()?) as usize;
        let end = self
            .at
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| "unreadable record: end of input".to_string())?;
        let bytes = &self.bytes[self.at..end];
        self.at = end;
        let malformed = || "unreadable record: malformed input".to_string();
        let continuation = |b: Option<&u8>| b.filter(|b| *b & 0xc0 == 0x80).map(|b| u16::from(*b & 0x3f)).ok_or_else(malformed);
        let mut units = Vec::with_capacity(length);
        let mut i = 0;
        while i < bytes.len() {
            let c = bytes[i];
            match c >> 4 {
                0..=7 => {
                    units.push(u16::from(c));
                    i += 1;
                }
                12 | 13 => {
                    let c2 = continuation(bytes.get(i + 1))?;
                    units.push((u16::from(c & 0x1f) << 6) | c2);
                    i += 2;
                }
                14 => {
                    let c2 = continuation(bytes.get(i + 1))?;
                    let c3 = continuation(bytes.get(i + 2))?;
                    units.push((u16::from(c & 0x0f) << 12) | (c2 << 6) | c3);
                    i += 3;
                }
                _ => return Err(malformed()),
            }
        }
        String::from_utf16(&units).map_err(|_| "unreadable record: a lone surrogate".to_string())
    }
}

fn read_u128_account(input: &mut Reader<'_>) -> Result<AccountId, String> {
    Ok(AccountId::new(input.u128()?))
}

fn read_key(input: &mut Reader<'_>) -> Result<ClientKey, String> {
    let created_at = WallTime::new(input.i64()?);
    let body = input.utf()?;
    ClientKey::try_new(created_at, body).map_err(|e| format!("unreadable record: {e}"))
}

fn read_amount(input: &mut Reader<'_>) -> Result<CaptureAmount, String> {
    match input.i8()? {
        0 => Ok(CaptureAmount::Full),
        1 => CaptureAmount::try_partial(input.i64()?).map_err(|e| format!("unreadable record: {e}")),
        tag => Err(unknown("capture amount", tag)),
    }
}

fn read_command(input: &mut Reader<'_>) -> Result<Command, String> {
    match input.i8()? {
        0 => {
            let key = read_key(input)?;
            let debit = read_u128_account(input)?;
            let credit = read_u128_account(input)?;
            let amount = input.u128()?;
            let ledger = input.i32()?;
            let hold_timeout_seconds = input.i32()?;
            Ok(Command::Authorize(AuthorizeCommand { key, debit, credit, amount, ledger, hold_timeout_seconds }))
        }
        1 => Ok(Command::Cancel(read_key(input)?)),
        2 => {
            let key = read_key(input)?;
            Ok(Command::Capture(key, read_amount(input)?))
        }
        tag => Err(unknown("command", tag)),
    }
}

fn read_status(input: &mut Reader<'_>) -> Result<TbStatus, String> {
    let name = input.utf()?;
    TbStatus::from_name(&name).ok_or_else(|| format!("unreadable record: no status named {name}"))
}

fn read_outcome(input: &mut Reader<'_>) -> Result<Outcome, String> {
    Ok(match input.i8()? {
        0 => Outcome::Auth(AuthOutcome::Approved),
        1 => Outcome::Auth(AuthOutcome::Declined(match input.i8()? {
            0 => DeclineReason::Unknown,
            1 => DeclineReason::NotExecuted,
            2 => DeclineReason::KeyReused(read_status(input)?),
            3 => DeclineReason::Ledger(read_status(input)?),
            tag => return Err(unknown("decline reason", tag)),
        })),
        2 => Outcome::Cancel(CancelOutcome::Cancelled),
        3 => Outcome::Cancel(CancelOutcome::CancelRefused),
        4 => Outcome::Capture(CaptureOutcome::Captured(read_amount(input)?)),
        5 => Outcome::Capture(CaptureOutcome::CaptureRefused(match input.i8()? {
            0 => CaptureRefusal::NotApproved,
            1 => CaptureRefusal::Cancelled,
            2 => CaptureRefusal::Expired,
            3 => CaptureRefusal::ExceedsHold,
            4 => CaptureRefusal::KeyReused(read_status(input)?),
            5 => CaptureRefusal::Ledger(read_status(input)?),
            tag => return Err(unknown("capture refusal", tag)),
        })),
        tag => return Err(unknown("outcome", tag)),
    })
}
