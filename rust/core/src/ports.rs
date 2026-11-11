//! The ports: the calls and replies that cross between the core and the world (README section 11, ports). Port of
//! `Ports.scala`.

use alloc::collections::BTreeSet;
use alloc::vec::Vec;

use crate::domain::{AccountId, Record};
use crate::machine::LogTime;
use crate::partitioner::{self, Offset, Partition};
use crate::periods::Period;
use crate::tb::{TbStatus, Transfer};

/// A caller's connection to an edge instance: the caller API's handle, on which the answer goes back (README
/// section 9). The interpreter chooses it, unique for the instance's life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Connection(i64);

impl Connection {
    pub const fn new(n: i64) -> Connection {
        Connection(n)
    }

    pub const fn value(self) -> i64 {
        self.0
    }
}

/// The command log port: one Kafka topic, keyed by `A`. The edge appends commands and outcomes and follows the
/// producer's metadata; the resolver reads the topic as a consumer, appends outcomes and commits offsets.
pub mod command_log {
    use super::*;

    /// Append a record to the partition of its key, `A`, and wait for the in-sync replicas (README section 4).
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct Append {
        pub record: Record,
    }

    impl Append {
        pub fn partition(&self, partitions: u32) -> Partition {
            partitioner::partition(self.record.command().a(), partitions)
        }
    }

    /// The reply to an append.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum Written {
        Acknowledged,
        /// Not acknowledged, for example after a timeout: the record may still land (README section 5).
        Failed,
    }

    /// The partitions the producer's metadata shows without a leader (README section 8, failover).
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct Leaders {
        pub leaderless: BTreeSet<Partition>,
    }

    /// What the consumer tells the resolver.
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub enum Delivery {
        /// The resolver reads the partition again from its committed offset.
        Assigned(Partition, Offset),
        Revoked(Partition),
        /// A record, with the append time its partition leader stamped on it (README section 4, command log rules).
        Read(Partition, Offset, LogTime, Record),
        /// A poll found nothing past the last record read: the end of the partition (README section 6). The consumer
        /// keeps polling an idle partition, since the resolver judges an idle partition's commands only when it hears
        /// this (README section 8, stragglers).
        ReachedEnd(Partition),
    }

    /// Commit the offset to read again from. The group coordinator rejects it once a rebalance has moved the
    /// partition away, and the resolver needs no reply (README section 5, zombie resolver).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Commit {
        pub partition: Partition,
        pub next: Offset,
    }
}

/// The ledger port: TigerBeetle, one cluster per period (README section 4).
pub mod ledger {
    use super::*;

    /// One `create_transfers` call to the cluster of a period, through the client the process shares. The client
    /// packs the events of calls made while its previous request is in flight into its next request (README
    /// section 6).
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct Submit {
        pub cluster: Period,
        pub transfers: Vec<Transfer>,
    }

    /// The reply to a call: the port of Scala's `Ledger.Result`.
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub enum CallResult {
        /// One status per transfer, in order.
        Returned(Vec<TbStatus>),
        /// The call failed without statuses, so its effect is unknown.
        Failed,
    }

    /// The ledger feed: up to `limit` transfers of a cluster with a timestamp after `after`, in timestamp order.
    /// The scan after each run reads it to the end. No machine reads it.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Scan {
        pub cluster: Period,
        pub after: i64,
        pub limit: i32,
    }

    /// Accounts to set up in a cluster, such as the two fence accounts. No machine does this.
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct CreateAccounts {
        pub cluster: Period,
        pub accounts: Vec<NewAccount>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct NewAccount {
        pub id: AccountId,
        pub ledger: i32,
        pub code: i32,
        pub debits_must_not_exceed_credits: bool,
        pub credits_must_not_exceed_debits: bool,
    }

    /// Read the balances of accounts: how a hold's expiry shows from outside. No machine does this.
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct LookupAccounts {
        pub cluster: Period,
        pub ids: Vec<AccountId>,
    }

    /// Close an account, as an operator might under a live hold. No machine does this.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct CloseAccount {
        pub cluster: Period,
        pub id: AccountId,
        pub other: AccountId,
        pub ledger: i32,
        pub code: i32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Balances {
        pub id: AccountId,
        pub debits_pending: u128,
        pub debits_posted: u128,
        pub credits_pending: u128,
        pub credits_posted: u128,
    }
}
