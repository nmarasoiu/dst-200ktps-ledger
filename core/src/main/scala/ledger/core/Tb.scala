package ledger.core

/** TigerBeetle's result codes for a transfer, as `CreateTransferStatus` in `src/tigerbeetle.zig` lists them, with
  * their numeric values. A closed type: every rule matches it exhaustively through `group`.
  */
enum TbStatus(val code: Long) derives CanEqual:
  case Created extends TbStatus(0xffffffffL)
  case DeprecatedOk extends TbStatus(0)
  case LinkedEventFailed extends TbStatus(1)
  case LinkedEventChainOpen extends TbStatus(2)
  case ImportedEventExpected extends TbStatus(56)
  case ImportedEventNotExpected extends TbStatus(57)
  case TimestampMustBeZero extends TbStatus(3)
  case ImportedEventTimestampOutOfRange extends TbStatus(58)
  case ImportedEventTimestampMustNotAdvance extends TbStatus(59)
  case ReservedFlag extends TbStatus(4)
  case IdMustNotBeZero extends TbStatus(5)
  case IdMustNotBeIntMax extends TbStatus(6)
  case ExistsWithDifferentFlags extends TbStatus(36)
  case ExistsWithDifferentPendingId extends TbStatus(40)
  case ExistsWithDifferentTimeout extends TbStatus(44)
  case ExistsWithDifferentDebitAccountId extends TbStatus(37)
  case ExistsWithDifferentCreditAccountId extends TbStatus(38)
  case ExistsWithDifferentAmount extends TbStatus(39)
  case ExistsWithDifferentUserData128 extends TbStatus(41)
  case ExistsWithDifferentUserData64 extends TbStatus(42)
  case ExistsWithDifferentUserData32 extends TbStatus(43)
  case ExistsWithDifferentLedger extends TbStatus(67)
  case ExistsWithDifferentCode extends TbStatus(45)
  case Exists extends TbStatus(46)
  case IdAlreadyFailed extends TbStatus(68)
  case FlagsAreMutuallyExclusive extends TbStatus(7)
  case DebitAccountIdMustNotBeZero extends TbStatus(8)
  case DebitAccountIdMustNotBeIntMax extends TbStatus(9)
  case CreditAccountIdMustNotBeZero extends TbStatus(10)
  case CreditAccountIdMustNotBeIntMax extends TbStatus(11)
  case AccountsMustBeDifferent extends TbStatus(12)
  case PendingIdMustBeZero extends TbStatus(13)
  case PendingIdMustNotBeZero extends TbStatus(14)
  case PendingIdMustNotBeIntMax extends TbStatus(15)
  case PendingIdMustBeDifferent extends TbStatus(16)
  case TimeoutReservedForPendingTransfer extends TbStatus(17)
  case ClosingTransferMustBePending extends TbStatus(64)
  case LedgerMustNotBeZero extends TbStatus(19)
  case CodeMustNotBeZero extends TbStatus(20)
  case DebitAccountNotFound extends TbStatus(21)
  case CreditAccountNotFound extends TbStatus(22)
  case AccountsMustHaveTheSameLedger extends TbStatus(23)
  case TransferMustHaveTheSameLedgerAsAccounts extends TbStatus(24)
  case PendingTransferNotFound extends TbStatus(25)
  case PendingTransferNotPending extends TbStatus(26)
  case PendingTransferHasDifferentDebitAccountId extends TbStatus(27)
  case PendingTransferHasDifferentCreditAccountId extends TbStatus(28)
  case PendingTransferHasDifferentLedger extends TbStatus(29)
  case PendingTransferHasDifferentCode extends TbStatus(30)
  case ExceedsPendingTransferAmount extends TbStatus(31)
  case PendingTransferHasDifferentAmount extends TbStatus(32)
  case PendingTransferAlreadyPosted extends TbStatus(33)
  case PendingTransferAlreadyVoided extends TbStatus(34)
  case PendingTransferExpired extends TbStatus(35)
  case ImportedEventTimestampMustNotRegress extends TbStatus(60)
  case ImportedEventTimestampMustPostdateDebitAccount extends TbStatus(61)
  case ImportedEventTimestampMustPostdateCreditAccount extends TbStatus(62)
  case ImportedEventTimeoutMustBeZero extends TbStatus(63)
  case DebitAccountAlreadyClosed extends TbStatus(65)
  case CreditAccountAlreadyClosed extends TbStatus(66)
  case OverflowsDebitsPending extends TbStatus(47)
  case OverflowsCreditsPending extends TbStatus(48)
  case OverflowsDebitsPosted extends TbStatus(49)
  case OverflowsCreditsPosted extends TbStatus(50)
  case OverflowsDebits extends TbStatus(51)
  case OverflowsCredits extends TbStatus(52)
  case OverflowsTimeout extends TbStatus(53)
  case ExceedsCredits extends TbStatus(54)
  case ExceedsDebits extends TbStatus(55)
  case Deprecated18 extends TbStatus(18)

  /** What the code means to the rules. No wildcard: a new code fails the build here until it is placed. */
  def group: TbGroup = this match
    case Created => TbGroup.Created
    case Exists => TbGroup.Exists
    case ExistsWithDifferentFlags => TbGroup.ExistsWithDifferentFlags
    case ExistsWithDifferentPendingId | ExistsWithDifferentTimeout | ExistsWithDifferentDebitAccountId |
        ExistsWithDifferentCreditAccountId | ExistsWithDifferentAmount | ExistsWithDifferentUserData128 |
        ExistsWithDifferentUserData64 | ExistsWithDifferentUserData32 | ExistsWithDifferentLedger |
        ExistsWithDifferentCode =>
      TbGroup.ExistsWithDifferentField(this)
    case IdAlreadyFailed => TbGroup.IdAlreadyFailed
    // Exactly these seven are transient and burn the ID (README section 6, `CreateTransferStatus.transient`).
    case DebitAccountNotFound | CreditAccountNotFound | PendingTransferNotFound | ExceedsCredits | ExceedsDebits |
        DebitAccountAlreadyClosed | CreditAccountAlreadyClosed =>
      TbGroup.Transient(this)
    case PendingTransferNotPending => TbGroup.PendingNotPending
    case PendingTransferAlreadyPosted => TbGroup.AlreadyPosted
    case PendingTransferAlreadyVoided => TbGroup.AlreadyVoided
    case PendingTransferExpired => TbGroup.PendingExpired
    case ExceedsPendingTransferAmount => TbGroup.ExceedsPendingAmount
    case DeprecatedOk | LinkedEventFailed | LinkedEventChainOpen | ImportedEventExpected | ImportedEventNotExpected |
        TimestampMustBeZero | ImportedEventTimestampOutOfRange | ImportedEventTimestampMustNotAdvance | ReservedFlag |
        IdMustNotBeZero | IdMustNotBeIntMax | FlagsAreMutuallyExclusive | DebitAccountIdMustNotBeZero |
        DebitAccountIdMustNotBeIntMax | CreditAccountIdMustNotBeZero | CreditAccountIdMustNotBeIntMax |
        AccountsMustBeDifferent | PendingIdMustBeZero | PendingIdMustNotBeZero | PendingIdMustNotBeIntMax |
        PendingIdMustBeDifferent | TimeoutReservedForPendingTransfer | ClosingTransferMustBePending |
        LedgerMustNotBeZero | CodeMustNotBeZero | AccountsMustHaveTheSameLedger |
        TransferMustHaveTheSameLedgerAsAccounts | PendingTransferHasDifferentDebitAccountId |
        PendingTransferHasDifferentCreditAccountId | PendingTransferHasDifferentLedger |
        PendingTransferHasDifferentCode | PendingTransferHasDifferentAmount | ImportedEventTimestampMustNotRegress |
        ImportedEventTimestampMustPostdateDebitAccount | ImportedEventTimestampMustPostdateCreditAccount |
        ImportedEventTimeoutMustBeZero | OverflowsDebitsPending | OverflowsCreditsPending | OverflowsDebitsPosted |
        OverflowsCreditsPosted | OverflowsDebits | OverflowsCredits | OverflowsTimeout | Deprecated18 =>
      TbGroup.Rejected(this)

/** Result codes grouped by what they tell the rules. */
enum TbGroup derives CanEqual:
  case Created
  case Exists
  /** Only a fence can cause this on an authorization: flags are compared first (README section 5). */
  case ExistsWithDifferentFlags
  case ExistsWithDifferentField(status: TbStatus)
  case IdAlreadyFailed
  /** One of the seven codes that burn the ID: it can never succeed again. */
  case Transient(status: TbStatus)
  case PendingNotPending
  case AlreadyPosted
  case AlreadyVoided
  case PendingExpired
  /** Not recorded, so a corrected request may reuse the ID. */
  case ExceedsPendingAmount
  /** A validation failure, not recorded: the ID stays free. */
  case Rejected(status: TbStatus)

enum TransferFlag derives CanEqual:
  case Pending, PostPendingTransfer, VoidPendingTransfer

/** The fields of a TigerBeetle transfer the pipeline sets. User data stays 0. */
final case class Transfer(
    id: TransferId,
    debit: AccountId,
    credit: AccountId,
    amount: U128,
    pendingId: TransferId,
    ledger: Int,
    code: Int,
    flags: Set[TransferFlag],
    timeoutSeconds: Int,
) derives CanEqual:
  def isPending: Boolean = flags(TransferFlag.Pending)
  def isPost: Boolean = flags(TransferFlag.PostPendingTransfer)
  def isVoid: Boolean = flags(TransferFlag.VoidPendingTransfer)

/** What the edge and the resolver put into the transfers they build. The fence's fields are fixed for all time,
  * like the ID layout: every fence is identical, so a fence that meets an earlier one returns `exists` (README
  * section 8, cancels).
  */
final case class LedgerConfig(
    fenceDebit: AccountId,
    fenceCredit: AccountId,
    fenceLedger: Int,
    fenceCode: Int,
    authorizationCode: Int,
)

object Transfers:
  /** A pending transfer with ID `A` and the hold timeout the edge stamped. */
  def authorization(c: Command.Authorize, cfg: LedgerConfig): Transfer =
    Transfer(c.a, c.debit, c.credit, c.amount, TransferId.Zero, c.ledger, cfg.authorizationCode, Set(TransferFlag.Pending), c.holdTimeoutSeconds)

  /** ID `A`, amount 0, no flags, between the two fence accounts (README section 8, fencing). */
  def fence(a: TransferId, cfg: LedgerConfig): Transfer =
    Transfer(a, cfg.fenceDebit, cfg.fenceCredit, U128.Zero, TransferId.Zero, cfg.fenceLedger, cfg.fenceCode, Set.empty, 0)

  /** ID `V`, amount 0 for the full pending amount; accounts, ledger and code inherit from the hold. */
  def void(a: TransferId): Transfer =
    Transfer(Ids.void(a), AccountId.Zero, AccountId.Zero, U128.Zero, a, 0, 0, Set(TransferFlag.VoidPendingTransfer), 0)

  /** ID `P`. Full is sent as the maximum amount, since a post of 0 posts nothing (README section 8, captures). */
  def post(a: TransferId, amount: CaptureAmount): Transfer =
    val value = amount match
      case CaptureAmount.Full => U128.Max
      case CaptureAmount.Partial(n) => U128.of(n)
    Transfer(Ids.post(a), AccountId.Zero, AccountId.Zero, value, a, 0, 0, Set(TransferFlag.PostPendingTransfer), 0)

  /** A cancel: one batch of two unlinked events, the fence first. */
  def cancel(a: TransferId, cfg: LedgerConfig): Vector[Transfer] = Vector(fence(a, cfg), void(a))

  /** What the edge sends. An authorization whose period has ended gets the fence instead (README section 4). */
  def forEdge(command: Command, fencePath: Boolean, cfg: LedgerConfig): Vector[Transfer] = command match
    case c: Command.Authorize => Vector(if fencePath then fence(c.a, cfg) else authorization(c, cfg))
    case c: Command.Cancel => cancel(c.a, cfg)
    case c: Command.Capture => Vector(post(c.a, c.amount))

  /** What the resolver sends. No branch builds an authorization: the resolver fences it (README section 8). */
  def forResolver(command: Command, cfg: LedgerConfig): Vector[Transfer] = command match
    case c: Command.Authorize => Vector(fence(c.a, cfg))
    case c: Command.Cancel => cancel(c.a, cfg)
    case c: Command.Capture => Vector(post(c.a, c.amount))
