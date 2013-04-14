package com.palmercox.cassandra

import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.util.UUID

import akka.actor.IO.Iteratee
import akka.actor.IO.take
import akka.util.ByteIterator
import akka.util.ByteString
import akka.util.ByteStringBuilder

object CassandraDefs {
  sealed trait OptionFlag
  final case object CompressedOption extends OptionFlag
  final case object TracingOption extends OptionFlag

  sealed trait OpcodeType
  final case object ErrorOpcode extends OpcodeType
  final case object StartupOpcode extends OpcodeType
  final case object ReadyOpcode extends OpcodeType
  final case object AuthenticateOpcode extends OpcodeType
  final case object CredentialsOpcode extends OpcodeType
  final case object OptionsOpcode extends OpcodeType
  final case object SupportedOpcode extends OpcodeType
  final case object QueryOpcode extends OpcodeType
  final case object ResultOpcode extends OpcodeType
  final case object PrepareOpcode extends OpcodeType
  final case object ExecuteOpcode extends OpcodeType
  final case object RegisterOpcode extends OpcodeType
  final case object EventOpcode extends OpcodeType

  sealed trait ConsistencyType
  final case object AnyConsistency extends ConsistencyType
  final case object OneConsistency extends ConsistencyType
  final case object TwoConsistency extends ConsistencyType
  final case object ThreeConsistency extends ConsistencyType
  final case object QuorumConsistency extends ConsistencyType
  final case object AllConsistency extends ConsistencyType
  final case object LocalQuorumConsistency extends ConsistencyType
  final case object EachQuorumConsistency extends ConsistencyType

  sealed trait ResultKind
  final case object VoidKind extends ResultKind
  final case object RowsKind extends ResultKind
  final case object SetKeyspaceKind extends ResultKind
  final case object PreparedKind extends ResultKind
  final case object SchemaChangeKind extends ResultKind

  sealed trait RowsResultFlag
  final case object GlobalTableSpec extends RowsResultFlag

  sealed trait RowValueType
  final case object CustomRowValueType extends RowValueType
  final case object AsciiRowValueType extends RowValueType
  final case object BigintRowValueType extends RowValueType
  final case object BlobRowValueType extends RowValueType
  final case object BooleanRowValueType extends RowValueType
  final case object CounterRowValueType extends RowValueType
  final case object DecimalRowValueType extends RowValueType
  final case object DoubleRowValueType extends RowValueType
  final case object FloatRowValueType extends RowValueType
  final case object IntRowValueType extends RowValueType
  final case object TextRowValueType extends RowValueType
  final case object TimestampRowValueType extends RowValueType
  final case object UuidRowValueType extends RowValueType
  final case object VarcharRowValueType extends RowValueType
  final case object VarintRowValueType extends RowValueType
  final case object TimeuuidRowValueType extends RowValueType
  final case object InetRowValueType extends RowValueType
  final case class ListRowValueType(elemType: RowValueType) extends RowValueType
  final case class MapRowValueType(keyType: RowValueType, valType: RowValueType) extends RowValueType
  final case class SetRowValueType(elemType: RowValueType) extends RowValueType

  //  sealed trait RowValue
  //  case class CustomRowValue(value: AnyRef) extends RowValue
  //  case class AsciiRowValue(value: String) extends RowValue
  //  case class LongRowValue(value: Long) extends RowValue
  //  case class BlobRowValue(value: Array[Byte]) extends RowValue
  //  case class BooleanRowValue(value: Boolean) extends RowValue
  //  case class CounterRowValue(value: AnyRef) extends RowValue
  //  case class DecimalRowValue(value: AnyRef) extends RowValue
  //  case class DoubleRowValue(value: Double) extends RowValue
  //  case class FloatRowValue(value: Float) extends RowValue
  //  case class IntRowValue(value: Int) extends RowValue
  //  case class TextRowValue(value: String) extends RowValue
  //  case class TimestampRowValue(value: AnyRef) extends RowValue
  //  case class UUIDRowValue(value: UUID) extends RowValue
  //  case class VarcharRowValue(value: String) extends RowValue
  //  case class VarintRowValue(value: AnyRef) extends RowValue
  //  case class TimeuuidRowValue(value: UUID) extends RowValue
  //  case class InetRowValue(value: InetAddress) extends RowValue
  //  case class ListRowValue[A](value: List[A]) extends RowValue
  //  case class MapRowValue[K, V](value: Map[K, V]) extends RowValue
  //  case class SetRowValue[E](value: Set[E]) extends RowValue

  //
  // case classes for all the message structures supported by the protocol
  //

  final case class Header(
    val version: Byte,
    val flags: Set[OptionFlag],
    val stream: Byte,
    val opcode: OpcodeType,
    val length: Int)

  sealed trait ResponseMessage

  final case class ErrorMessage(errorCode: Int, errorMessage: String) extends ResponseMessage
  final case object ReadyMessage extends ResponseMessage
  final case class AuthenticateMessage(authenticator: String) extends ResponseMessage
  final case class SupportedMessage(supportedOptions: Map[String, Seq[String]]) extends ResponseMessage

  final case class RowSpec(keyspace: String, tablespace: String, columnName: String, columnType: RowValueType)
  final case class RowsResultMetadata(flags: Set[RowsResultFlag], columnCount: Int, columnSpecs: Seq[RowSpec])

  sealed trait ResultMessage extends ResponseMessage
  final case object VoidResultMessage extends ResultMessage
  final case class RowsResultMessage(rowCount: Int, rows: Seq[Seq[Any]]) extends ResultMessage

  final case class SetKeyspaceResultMessage(keyspace: String) extends ResultMessage
  final case class PrepareResultMessage(id: Array[Byte], metadata: RowsResultMetadata) extends ResultMessage
  final case class SchemaChangeResultMessage(change: String, keyspace: String, table: String) extends ResultMessage

  sealed trait EventMessage extends ResponseMessage
  final case class TopologyChangeEvent(changeType: String, address: InetAddress) extends EventMessage
  final case class StatusChangeEvent(changeType: String, address: InetAddress) extends EventMessage
  final case class SchemaChangeEvent(action: String, keyspace: String, table: String) extends EventMessage

  sealed trait RequestMessage
  final case class StartupMessage(options: Map[String, String]) extends RequestMessage
  final case class CredentialsMessage(params: Map[String, String]) extends RequestMessage
  final case object OptionsMessage extends RequestMessage
  final case class QueryMessage(query: String, consistency: ConsistencyType) extends RequestMessage
  final case class PrepareMessage(query: String) extends RequestMessage
  final case class ExecuteMessage(queryId: Array[Byte], params: List[Any], consistency: ConsistencyType) extends RequestMessage
  // TODO - objects instead of strings!
  final case class RegisterMessage(types: Seq[String]) extends RequestMessage
}

private object CassandraCodec {
  import CassandraDefs._

  //
  // Functions for parsing the datatypes used in the protocol
  //

  def decodeOptions(options: Byte): Set[OptionFlag] = {
    var optionFlags = Set[OptionFlag]()
    if ((options & 0x01) != 0) optionFlags = optionFlags + CompressedOption
    if ((options & 0x02) != 0) optionFlags = optionFlags + TracingOption
    optionFlags
  }

  def encodeOptions(options: Set[OptionFlag]): Byte = {
    var result = 0
    if (options.contains(CompressedOption)) result |= 0x01
    if (options.contains(TracingOption)) result |= 0x02
    result.toByte
  }

  def decodeOpcode(opcode: Byte): OpcodeType = opcode match {
    case 0x00 => ErrorOpcode
    case 0x01 => StartupOpcode
    case 0x02 => ReadyOpcode
    case 0x03 => AuthenticateOpcode
    case 0x04 => CredentialsOpcode
    case 0x05 => OptionsOpcode
    case 0x06 => SupportedOpcode
    case 0x07 => QueryOpcode
    case 0x08 => ResultOpcode
    case 0x09 => PrepareOpcode
    case 0x0A => ExecuteOpcode
    case 0x0B => RegisterOpcode
    case 0x0C => EventOpcode
    case _ => ???
  }

  def encodeOpcode(opcode: OpcodeType): Byte = opcode match {
    case ErrorOpcode => 0x00
    case StartupOpcode => 0x01
    case ReadyOpcode => 0x02
    case AuthenticateOpcode => 0x03
    case CredentialsOpcode => 0x04
    case OptionsOpcode => 0x05
    case SupportedOpcode => 0x06
    case QueryOpcode => 0x07
    case ResultOpcode => 0x08
    case PrepareOpcode => 0x09
    case ExecuteOpcode => 0x0A
    case RegisterOpcode => 0x0B
    case EventOpcode => 0x0C
  }

  def decodeConsistency(consistency: Short): ConsistencyType = consistency match {
    case 0x00 => AnyConsistency
    case 0x01 => OneConsistency
    case 0x02 => TwoConsistency
    case 0x03 => ThreeConsistency
    case 0x04 => QuorumConsistency
    case 0x05 => AllConsistency
    case 0x06 => LocalQuorumConsistency
    case 0x07 => EachQuorumConsistency
    case _ => ???
  }

  def encodeConsistency(consistency: ConsistencyType): Short = consistency match {
    case AnyConsistency => 0x00
    case OneConsistency => 0x01
    case TwoConsistency => 0x02
    case ThreeConsistency => 0x03
    case QuorumConsistency => 0x04
    case AllConsistency => 0x05
    case LocalQuorumConsistency => 0x06
    case EachQuorumConsistency => 0x07
  }

  def decodeResultKind(resultKind: Int): ResultKind = resultKind match {
    case 0x01 => VoidKind
    case 0x02 => RowsKind
    case 0x03 => SetKeyspaceKind
    case 0x04 => PreparedKind
    case 0x05 => SchemaChangeKind
    case _ => ???
  }

  def encodeResultKind(resultKind: ResultKind): Int = resultKind match {
    case VoidKind => 0x01
    case RowsKind => 0x02
    case SetKeyspaceKind => 0x03
    case PreparedKind => 0x04
    case SchemaChangeKind => 0x05
  }

  def decodeRowsResultFlags(flags: Int): Set[RowsResultFlag] = {
    var result = Set[RowsResultFlag]()
    if ((flags & 0x01) != 0) result = result + GlobalTableSpec
    result
  }

  def encodeRowsResultFlags(flags: Set[RowsResultFlag]): Int = {
    var result = 0
    if (flags.contains(GlobalTableSpec)) result |= 0x01
    result
  }

  def decodeRowValueType(rowValueType: Short, next: () => Short): RowValueType = rowValueType match {
    case 0x00 => CustomRowValueType
    case 0x01 => AsciiRowValueType
    case 0x02 => BigintRowValueType
    case 0x03 => BlobRowValueType
    case 0x04 => BooleanRowValueType
    case 0x05 => CounterRowValueType
    case 0x06 => DecimalRowValueType
    case 0x07 => DoubleRowValueType
    case 0x08 => FloatRowValueType
    case 0x09 => IntRowValueType
    case 0x0A => TextRowValueType
    case 0x0B => TimestampRowValueType
    case 0x0C => UuidRowValueType
    case 0x0D => VarcharRowValueType
    case 0x0E => VarintRowValueType
    case 0x0F => TimeuuidRowValueType
    case 0x10 => InetRowValueType
    case 0x20 => ListRowValueType(decodeRowValueType(next(), next))
    case 0x21 => MapRowValueType(decodeRowValueType(next(), next), decodeRowValueType(next(), next))
    case 0x22 => SetRowValueType(decodeRowValueType(next(), next))
    case _ => ???
  }

  def encodeRowValueType(rowValueType: RowValueType): Short = rowValueType match {
    case CustomRowValueType => 0x00
    case AsciiRowValueType => 0x01
    case BigintRowValueType => 0x02
    case BlobRowValueType => 0x03
    case BooleanRowValueType => 0x04
    case CounterRowValueType => 0x05
    case DecimalRowValueType => 0x06
    case DoubleRowValueType => 0x07
    case FloatRowValueType => 0x08
    case IntRowValueType => 0x09
    case TextRowValueType => 0x0A
    case TimestampRowValueType => 0x0B
    case UuidRowValueType => 0x0C
    case VarcharRowValueType => 0x0D
    case VarintRowValueType => 0x0E
    case TimeuuidRowValueType => 0x0F
    case InetRowValueType => 0x10
    case ListRowValueType(_) => 0x20
    case MapRowValueType(_, _) => 0x21
    case SetRowValueType(_) => 0x22
  }

  private implicit val bo = ByteOrder.BIG_ENDIAN

  def getByte(implicit it: ByteIterator): Byte = it.getByte
  def putByte(value: Byte)(implicit bsb: ByteStringBuilder) { bsb.putByte(value) }
  def getRawBytes(len: Int)(implicit it: ByteIterator): Array[Byte] = {
    val bytes = new Array[Byte](len)
    it.getBytes(bytes)
    bytes
  }
  def putRawBytes(value: Array[Byte])(implicit bsb: ByteStringBuilder) { bsb.putBytes(value) }
  def getShort(implicit it: ByteIterator): Short = it.getShort
  def putShort(value: Short)(implicit bsb: ByteStringBuilder) { bsb.putShort(value) }
  def getInt(implicit it: ByteIterator): Int = it.getInt
  def putInt(value: Int)(implicit bsb: ByteStringBuilder) { bsb.putInt(value) }
  def getLong(implicit it: ByteIterator): Long = it.getLong
  def getFloat(implicit it: ByteIterator): Float = it.getFloat
  def getDouble(implicit it: ByteIterator): Double = it.getDouble
  def getString(implicit it: ByteIterator): String = {
    val len = getShort
    val bytes = new Array[Byte](len)
    it.getBytes(bytes)
    Charset.forName("UTF-8").decode(ByteBuffer.wrap(bytes)).toString()
  }
  def putString(value: String)(implicit bsb: ByteStringBuilder) {
    val bytes = value.getBytes("UTF-8")
    putShort(bytes.length.toShort)
    putRawBytes(bytes)
  }
  def getLongString(implicit it: ByteIterator): String = {
    val len = getInt
    val bytes = new Array[Byte](len)
    it.getBytes(bytes)
    Charset.forName("UTF-8").decode(ByteBuffer.wrap(bytes)).toString()
  }
  def putLongString(value: String)(implicit bsb: ByteStringBuilder) {
    val bytes = value.getBytes("UTF-8")
    putInt(bytes.length)
    putRawBytes(bytes)
  }
  def getRawString(len: Int)(implicit it: ByteIterator): String = {
    val bytes = new Array[Byte](len)
    it.getBytes(bytes)
    Charset.forName("UTF-8").decode(ByteBuffer.wrap(bytes)).toString()
  }
  def getUUID(implicit it: ByteIterator): UUID = {
    val bytes = getRawBytes(16)
    UUID.nameUUIDFromBytes(bytes)
  }
  def getStringList(implicit it: ByteIterator): Seq[String] = {
    val count = getShort
    for {
      _ <- 1 to count
    } yield getString
  }
  def putStringList(value: Seq[String])(implicit bsb: ByteStringBuilder) {
    putShort(value.length.toShort)
    for (v <- value) {
      putString(v)
    }
  }
  def getBytes(implicit it: ByteIterator): Option[Array[Byte]] = {
    val len = getInt
    if (len >= 0)
      Some(getRawBytes(len))
    else
      None
  }
  def putBytes(value: Array[Byte])(implicit bsb: ByteStringBuilder) {
    if (value == null) {
      putInt(-1)
    } else {
      putInt(value.length)
      putRawBytes(value)
    }
  }
  def getShortBytes(implicit it: ByteIterator): Option[Array[Byte]] = {
    val len = getShort
    if (len >= 0)
      Some(getRawBytes(len))
    else
      None
  }
  def getInet(implicit it: ByteIterator): InetAddress = {
    val size = getByte
    val bytes = getRawBytes(size)
    InetAddress.getByAddress(bytes)
  }
  def getConsistency(implicit it: ByteIterator): ConsistencyType = {
    decodeConsistency(getShort)
  }
  def getMap(implicit it: ByteIterator): Map[String, String] = {
    val count = getShort
    Map() ++ (for {
      _ <- 1 to count
    } yield (getString -> getString))
  }
  def putMap(value: Map[String, String])(implicit bsb: ByteStringBuilder) {
    putShort(value.size.toShort)
    for ((k, v) <- value) {
      putString(k)
      putString(v)
    }
  }
  def getMultiMap(implicit it: ByteIterator): Map[String, Seq[String]] = {
    val count = getShort
    Map() ++ (for {
      _ <- 1 to count
    } yield (getString -> getStringList))
  }
}

private object CassandraMarshallingFuncs {
  import CassandraDefs._
  import CassandraCodec._

  private def marshallHeader(header: Header)(implicit bsb: ByteStringBuilder) {
    putByte(header.version)
    putByte(encodeOptions(header.flags))
    putByte(header.stream)
    putByte(encodeOpcode(header.opcode))
    putInt(header.length)
  }

  def marshallStartupMessage(startup: StartupMessage)(implicit bsb: ByteStringBuilder) {
    val m = startup.options + ("CQL_VERSION" -> "3.0.0")
    putMap(m)
  }
  def marshallCredentialsMessage(credentials: CredentialsMessage)(implicit bsb: ByteStringBuilder) {
    putMap(credentials.params)
  }
  def marshallQueryMessage(query: QueryMessage)(implicit bsb: ByteStringBuilder) {
    putLongString(query.query)
    putShort(encodeConsistency(query.consistency))
  }
  def marshallPrepareMessage(prepare: PrepareMessage)(implicit bsb: ByteStringBuilder) {
    putLongString(prepare.query)
  }
  def marshallExecuteMessage(execute: ExecuteMessage)(implicit bsb: ByteStringBuilder) {
    putBytes(execute.queryId)
    putShort(execute.params.length.toShort)
    // TODO: More types!
    for (p <- execute.params) {
      p match {
        case None => putInt(-1)
        case v: String => putBytes(v.getBytes("UTF-8"))
        case v: Int =>
          putInt(4)
          putInt(v)
        case v => throw new IllegalArgumentException("can't marshall: " + v.getClass())
      }
    }
    putShort(encodeConsistency(execute.consistency))
  }
  def marshallRegisterMessage(register: RegisterMessage)(implicit bsb: ByteStringBuilder) {
    putStringList(register.types)
  }

  def marshall(stream: Byte, request: RequestMessage): ByteString = {
    val bodyBuilder = new ByteStringBuilder()
    val opcode = request match {
      case m: StartupMessage =>
        marshallStartupMessage(m)(bodyBuilder)
        StartupOpcode
      case m: QueryMessage =>
        marshallQueryMessage(m)(bodyBuilder)
        QueryOpcode
      case m: CredentialsMessage =>
        marshallCredentialsMessage(m)(bodyBuilder)
        CredentialsOpcode
      case OptionsMessage =>
        OptionsOpcode
      case m: PrepareMessage =>
        marshallPrepareMessage(m)(bodyBuilder)
        PrepareOpcode
      case m: ExecuteMessage =>
        marshallExecuteMessage(m)(bodyBuilder)
        ExecuteOpcode
      case m: RegisterMessage =>
        marshallRegisterMessage(m)(bodyBuilder)
        RegisterOpcode
    }
    val headerBuilder = new ByteStringBuilder()
    val header = Header(0x01.toByte, Set(), stream, opcode, bodyBuilder.length)
    marshallHeader(header)(headerBuilder)
    headerBuilder.result ++ bodyBuilder.result
  }
}

private object CassandraParsingFuncs {
  import CassandraDefs._
  import CassandraCodec._

  //
  // functions to parse parts of a message into message objects
  //

  def parseHeader(headerBytes: ByteString): Header = {
    implicit val it = headerBytes.iterator
    val version = getByte
    val options = decodeOptions(getByte)
    val stream = getByte
    val opcode = decodeOpcode(getByte)
    val length = getInt
    Header(version, options, stream, opcode, length)
  }

  def parseRowsMetadata(implicit it: ByteIterator): RowsResultMetadata = {
    val flags = decodeRowsResultFlags(getInt)
    val columnsCount = getInt

    trait KeyTabReader {
      def getKeyspace: String
      def getTable: String
    }
    val keyTab = if (flags.contains(GlobalTableSpec)) {
      val keyspace = getString
      val table = getString
      new KeyTabReader {
        def getKeyspace: String = keyspace
        def getTable: String = table
      }
    } else {
      new KeyTabReader {
        def getKeyspace: String = getString
        def getTable: String = getString
      }
    }

    val rowSpecs = for {
      _ <- 1 to columnsCount
    } yield {
      val keyspace = keyTab.getKeyspace
      val table = keyTab.getTable
      val columName = getString
      val rowValueType = decodeRowValueType(getShort, () => getShort)
      RowSpec(keyspace, table, columName, rowValueType)
    }

    RowsResultMetadata(flags, columnsCount, rowSpecs)
  }

  def parseRowsResultMessage(implicit it: ByteIterator): RowsResultMessage = {
    val rowMetadata = parseRowsMetadata

    val rowsCount = getInt
    val rows = for {
      _ <- 1 to rowsCount
    } yield for {
      columnSpec <- rowMetadata.columnSpecs
    } yield {
      val len = getInt

      if (len >= 0) {
        columnSpec.columnType match {
          //          case CustomRowValueType => ???
          //          case AsciiRowValueType => TextRowValue(getLongString)
          //          case BigintRowValueType => LongRowValue(getLong)
          //          case BlobRowValueType => ???
          //          case BooleanRowValueType => BooleanRowValue(getInt != 0)
          //          case CounterRowValueType => ???
          //          case DecimalRowValueType => ???
          //          case DoubleRowValueType => DoubleRowValue(getDouble)
          //          case FloatRowValueType => FloatRowValue(getFloat)
          case IntRowValueType => getInt
          //          case TextRowValueType => getRawString(len)
          //          case TimestampRowValueType => ???
          //          case UuidRowValueType => UUIDRowValue(getUUID)
          case VarcharRowValueType => getRawString(len)
          //          case VarintRowValueType => ???
          //          case TimeuuidRowValueType => UUIDRowValue(getUUID)
          //          case InetRowValueType => InetRowValue(getInet)
          //          case ListRowValueType(_) => ???
          //          case MapRowValueType(_, _) => ???
          //          case SetRowValueType(_) => ???
          case _ => throw new Exception(s"Unfortunatley, parsing values of type $columnSpec.columnType is not implemented.")
        }
      } else {
        null
      }
    }

    RowsResultMessage(rowsCount, rows)
  }

  def parseResultMessage(implicit it: ByteIterator): ResultMessage = {
    val resultKind = decodeResultKind(getInt)
    resultKind match {
      case VoidKind => VoidResultMessage
      case RowsKind => parseRowsResultMessage
      case SetKeyspaceKind =>
        val keyspace = getString
        SetKeyspaceResultMessage(keyspace)
      case PreparedKind =>
        val prepareId = getShortBytes
        val rowMetadata = parseRowsMetadata
        PrepareResultMessage(prepareId.get, rowMetadata)
      case SchemaChangeKind =>
        val changeType = getString
        val keyspace = getString
        val table = getString
        SchemaChangeResultMessage(changeType, keyspace, table)
    }
  }

  def parseEventMessage(implicit it: ByteIterator): EventMessage = {
    val eventType = getString
    eventType match {
      case "TOPOLOGY_CHANGE" =>
        val changeType = getString
        val addr = getInet
        TopologyChangeEvent(changeType, addr)
      case "STATUS_CHANGE" =>
        val changeType = getString
        val addr = getInet
        StatusChangeEvent(changeType, addr)
      case "SCHEMA_CHANGE" =>
        val changeType = getString
        val keyspace = getString
        val table = getString
        SchemaChangeEvent(changeType, keyspace, table)
      case _ => ???
    }
  }

  def parseMessage(header: Header, messageBytes: ByteString): ResponseMessage = {
    implicit val h = header
    implicit val it = messageBytes.iterator
    val m = header.opcode match {
      case ErrorOpcode =>
        val code = getInt
        val message = getString
        ErrorMessage(code, message)
      case ReadyOpcode => ReadyMessage
      case AuthenticateOpcode =>
        val authenticator = getString
        AuthenticateMessage(authenticator)
      case SupportedOpcode =>
        val options = getMultiMap
        SupportedMessage(options)
      case ResultOpcode => parseResultMessage
      case EventOpcode => parseEventMessage
      case _ => throw new Exception("Received request message or unknown message.")
    }
    m
  }
}

object CassandraNativeParser {
  import akka.actor.IO.{ Iteratee, take }
  import CassandraDefs._
  import CassandraParsingFuncs._

  val parser: Iteratee[(Header, ResponseMessage)] = for {
    headerBytes <- take(8)
    header = parseHeader(headerBytes)
    messageBytes <- take(header.length)
  } yield (header, parseMessage(header, messageBytes))
}
