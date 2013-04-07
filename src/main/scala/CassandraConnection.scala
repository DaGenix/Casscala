package com.palmercox.casscala

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.IO
import akka.actor.IO.SocketHandle
import akka.actor.IOManager
import akka.actor.Props
import akka.actor.Status
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.ByteString
import akka.util.Timeout
import java.nio.ByteOrder
import akka.util.ByteStringBuilder
import java.net.InetAddress
import scala.collection.mutable.MultiMap
import java.util.UUID
import scala.collection.mutable.Set

sealed case class PingMessage(val message: String)

class CassandraConnection private (private val connActor: ActorRef)(
  implicit private val system: ActorSystem,
  private val timeout: Timeout = Timeout(60 seconds)) {

  import CassandraConnection.Message

  def ping(message: String): Future[Message] = {
    val f = connActor ? PingMessage(message)
    f.mapTo[Message]
  }

  def close() {
    system.stop(connActor)
  }
}

class CassandraConnectionActor(private val host: String, private val port: Int) extends Actor {
  import CassandraConnection.{ Message, readMessage }
  private implicit val byteOrder = ByteOrder.BIG_ENDIAN

  sealed case class WaiterInfo(ref: IO.IterateeRefSync[Message], requestor: ActorRef)

  private val socket: SocketHandle = IOManager(context.system).connect(host, port)

  private var waiters = Queue[WaiterInfo]()

  private var closed = false

  override def postStop = socket.close()

  private def ping(message: String) = {
    if (closed) {
      sender ! Status.Failure(new Exception("Conection is closed"))
    } else {
      val i = new ByteStringBuilder()
      i.putByte(0x01) // version
      i.putByte(0x00) // flags
      i.putByte(0x01) // stream
      i.putByte(0x01) // opcode - Startup
      i.putInt(2 + 2 + ("CQL_VERSION".getBytes("UTF-8").length) + 2 + ("3.0.0".getBytes("UTF-8").length)) // body length
      i.putShort(1)
      i.putShort("CQL_VERSION".getBytes("UTF-8").length)
      i.putBytes("CQL_VERSION".getBytes("UTF-8"))
      i.putShort("3.0.0".getBytes("UTF-8").length)
      i.putBytes("3.0.0".getBytes("UTF-8"))
      val b = i.result
      socket.write(b)
      waiters = waiters.enqueue(WaiterInfo(IO.IterateeRef.sync(readMessage), sender))
    }
  }

  @tailrec
  private def processWaiters(input: IO.Input) {
    if (!waiters.isEmpty) {
      val (waiter, remainingWaiters) = waiters.dequeue
      waiter.ref(input)
      waiter.ref.value match {
        case (IO.Done(value), remainingInput) =>
          waiters = remainingWaiters
          waiter.requestor ! value
          processWaiters(remainingInput)

        case (IO.Failure(f), remainingInput) =>
          waiters = remainingWaiters
          waiter.requestor ! Status.Failure(f)
          processWaiters(remainingInput)

        case (IO.Next(_), _) =>
      }
    }
  }

  def receive = {
    case PingMessage(message) =>
      ping(message)

    case IO.Read(socket, bytes) =>
      processWaiters(IO Chunk bytes)

    case IO.Closed(socket, cause) =>
      closed = true
      processWaiters(cause)

    case IO.Connected(socket, address) =>
    // Don't care

    case x =>
      println(x)
      ???
  }
}

object CassandraConnection {
  private implicit val byteOrder = ByteOrder.BIG_ENDIAN

  private val PROTOCOL_VERSION = 1

  def apply(host: String, port: Int)(implicit system: ActorSystem) = {
    val connActor = system.actorOf(Props(new CassandraConnectionActor(host, port)))
    new CassandraConnection(connActor)
  }

  private def utf8(bytes: ByteString): String = bytes.decodeString("UTF-8")

  object Direction extends Enumeration {
    type Direction = Value
    val Request = Value("Request")
    val Reply = Value("Reply")
  }
  import Direction._

  object Flag extends Enumeration {
    type Flag = Value
    val Compressed = Value("Compressed")
    val Tracing = Value("Tracing")
  }
  import Flag._

  object Opcode extends Enumeration {
    type Opcode = Value
    val Error = Value("Error")
    val Startup = Value("Startup")
    val Ready = Value("Ready")
    val Authenticate = Value("Authenticate")
    val Credentials = Value("Credentials")
    val Options = Value("Options")
    val Supported = Value("Supported")
    val Query = Value("Query")
    val Result = Value("Result")
    val Prepare = Value("Prepare")
    val Execute = Value("Execute")
    val Register = Value("Register")
    val Event = Value("Event")
  }
  import Opcode._

  object Consistency extends Enumeration {
    type Consistency = Value
    val Any = Value("Any")
    val One = Value("One")
    val Two = Value("Two")
    val Three = Value("Three")
    val Quorum = Value("Quorum")
    val All = Value("All")
    val LocalQuorum = Value("LocalQurum")
    val EachQuorum = Value("EachQuorum")
  }
  import Consistency._

  object ResultKind extends Enumeration {
    type ResultKind = Value
    val VoidKind = Value("Void")
    val RowsKind = Value("Rows")
    val SetKeyspaceKind = Value("SetKeyspace")
    val PreparedKind = Value("Prepared")
    val SchemaChangeKind = Value("SchemaChange")
  }
  import ResultKind._

  object RowsResultFlags extends Enumeration {
    type RowsResultFlags = Value
    val GlobalTableSpec = Value("GlobalTableSpec")
  }
  import RowsResultFlags._

  case class Header(
    val direction: Direction,
    val version: Int,
    val flags: List[Flag],
    val stream: Int,
    val opcode: Opcode,
    val length: Int)

  sealed trait Message

  case class ErrorMessage(
    header: Header,
    errorCode: Int,
    errorMessage: String) extends Message

  case class ReadyMessage(
    header: Header) extends Message

  case class AuthenticateMessage(
    header: Header,
    authenticator: String) extends Message

  case class SupportedMessage(
    header: Header,
    supportedOptions: MultiMap[String, String]) extends Message

  sealed trait ResultMessage extends Message

  case class VoidResultMessage(
    header: Header) extends ResultMessage

  sealed trait RowValue
  case class CustomRowValue(value: Array[Byte], rowType: String)
  case class AsciiRowValue(value: String) extends RowValue
  //  case class BigintRowValue(value: AnyRef) extends RowValue
  case class BlobRowValue(value: Array[Byte]) extends RowValue
  case class BooleanRowValue(value: Boolean) extends RowValue
  //  case class CounterRowValue(value: AnyRef) extends RowValue
  //  case class DecimalRowValue(value: AnyRef) extends RowValue
  case class DoubleRowValue(value: Double) extends RowValue
  case class FloatRowValue(value: Float) extends RowValue
  case class IntRowValue(value: Int) extends RowValue
  case class TextRowValue(value: String) extends RowValue
  //  case class TimestampRowValue(value: AnyRef) extends RowValue
  case class UuidRowValue(value: UUID) extends RowValue
  case class VarcharRowValue(value: String) extends RowValue
  //  case class VarintRowValue(value: AnyRef) extends RowValue
  case class TimeuuidRowValue(value: UUID) extends RowValue
  case class InetRowValue(value: InetAddress) extends RowValue
  case class ListRowValue[A](value: List[A]) extends RowValue
  case class MapRowValue[K, V](value: Map[K, V]) extends RowValue
  case class SetRowValue[E](value: Set[E]) extends RowValue

  case class KeyspaceSpec(
    keyspaceName: String,
    tableName: String)

  case class ColumnSpec(
    keyspaceSpec: KeyspaceSpec,
    columnName: String,
    columnType: Class[RowValue])

  case class RowsResultMetadata(
    flags: Set[RowsResultFlags],
    globalTableSpec: Option[KeyspaceSpec],
    columnCount: Int,
    columnSpecs: List[ColumnSpec])

  case class RowsResultMessage(
    header: Header,
    rowCount: Int,
    rows: List[List[Option[RowValue]]]) extends ResultMessage

  case class SetKeyspaceResultMessage(
    header: Header,
    keyspace: String) extends ResultMessage

  case class PrepareResultMessage(
    header: Header,
    id: Int,
    metadata: RowsResultMetadata) extends ResultMessage

  case class SchemaChangeResultMessage(
    header: Header,
    change: String,
    keyspace: String,
    table: String) extends ResultMessage

  def decodeVersion(b: Byte): (Direction, Int) =
    if ((b & 0x80) != 0) {
      (Reply, b & 0x7f)
    } else {
      (Request, b & 0x7f)
    }

  def readVersion: IO.Iteratee[(Direction, Int)] =
    for (b <- readByte) yield decodeVersion(b)

  def decodeFlags(f: Byte): List[Flag] = {
    var flags = List[Flag]()
    if ((f & 0x01) != 0) {
      flags = Compressed :: Compressed :: flags
    }
    if ((f & 0x02) != 0) {
      flags = Tracing :: Tracing :: flags
    }
    flags
  }

  def readFlags: IO.Iteratee[List[Flag]] =
    for (f <- readByte) yield decodeFlags(f)

  def readStreamNum: IO.Iteratee[Int] =
    for (s <- readByte) yield s

  def readOpcode: IO.Iteratee[Opcode] =
    for (o <- IO take 1) yield {
      o.head match {
        case 0x00 => Error
        case 0x01 => Startup
        case 0x02 => Ready
        case 0x03 => Authenticate
        case 0x04 => Credentials
        case 0x05 => Options
        case 0x06 => Supported
        case 0x07 => Query
        case 0x08 => Result
        case 0x09 => Prepare
        case 0x0A => Execute
        case 0x0B => Register
        case 0x0C => Event
      }
    }

  def readHeader: IO.Iteratee[Header] =
    for {
      (direction, version) <- readVersion
      flags <- readFlags
      streamNum <- readStreamNum
      opcode <- readOpcode
      length <- readInt
    } yield Header(direction, version, flags, streamNum, opcode, length)

  def readByte: IO.Iteratee[Byte] =
    for { i <- IO take 1 } yield i.iterator.head

  def readInt: IO.Iteratee[Int] =
    for { i <- IO take 4 } yield i.iterator.getInt

  def readShort: IO.Iteratee[Short] =
    for { s <- IO take 2 } yield s.iterator.getShort

  def readString: IO.Iteratee[String] =
    for {
      stringLength <- readShort
      stringBytes <- IO take stringLength
    } yield utf8(stringBytes)

  def readLongString: IO.Iteratee[String] =
    for {
      stringLength <- readInt
      stringBytes <- IO take stringLength
    } yield utf8(stringBytes)

  def readUUID: IO.Iteratee[ByteString] =
    IO take 16

  @tailrec
  def readStringListItems(itemsRemaining: Int, it: IO.Iteratee[List[String]]): IO.Iteratee[List[String]] = {
    if (itemsRemaining > 0) {
      val tmp = for {
        str <- readString
        l <- it
      } yield str :: str :: l
      readStringListItems(itemsRemaining - 1, tmp)
    } else {
      it
    }
  }

  def readStringList: IO.Iteratee[List[String]] =
    for {
      items <- readShort
      l <- readStringListItems(items, IO.Iteratee(List[String]()))
    } yield l

  def readBytesLength(length: Int): IO.Iteratee[Option[ByteString]] = {
    if (length >= 0) {
      for {
        b <- IO take length
      } yield Some(b)
    } else {
      IO.Iteratee(None)
    }
  }

  def readBytes: IO.Iteratee[Option[ByteString]] =
    for {
      l <- readInt
      b <- readBytesLength(l)
    } yield b

  def readShortBytes: IO.Iteratee[Option[ByteString]] =
    for {
      l <- readShort
      b <- readBytesLength(l)
    } yield b

  // TODO: readOption, readOptionList, readMultiMap

  def readInet: IO.Iteratee[InetAddress] =
    for {
      inetSize <- readByte
      addr <- IO take inetSize
    } yield InetAddress.getByAddress(addr.asByteBuffer.array())

  def readConsistency: IO.Iteratee[Consistency] =
    for {
      consistency <- readShort
    } yield consistency match {
      case 0x0000 => Any
      case 0x0001 => One
      case 0x0002 => Two
      case 0x0003 => Three
      case 0x0004 => Quorum
      case 0x0005 => All
      case 0x0006 => LocalQuorum
      case 0x0007 => EachQuorum
    }

  @tailrec
  def readMapListItems(itemsRemaining: Int, it: IO.Iteratee[Map[String, String]]): IO.Iteratee[Map[String, String]] = {
    if (itemsRemaining > 0) {
      val tmp = for {
        key <- readString
        value <- readString
        m <- it
      } yield m + (key -> value)
      readMapListItems(itemsRemaining - 1, tmp)
    } else {
      it
    }
  }

  def readMap: IO.Iteratee[Map[String, String]] =
    for {
      items <- readShort
      l <- readMapListItems(items, IO.Iteratee(Map[String, String]()))
    } yield l

  def readBody(length: Int): IO.Iteratee[ByteString] =
    IO take length

  def readErrorMessage(header: Header): IO.Iteratee[ErrorMessage] = {
    for {
      errorCode <- readInt
      errorMesssage <- readString
    } yield ErrorMessage(header, errorCode, errorMesssage)
  }

  def readReadyMessage(header: Header): IO.Iteratee[ReadyMessage] = {
    IO.Iteratee(ReadyMessage(header))
  }

  def readVoidResult(header: Header): IO.Iteratee[VoidResultMessage] =
    IO.Iteratee(VoidResultMessage(header))

  def readRowsResultFlags: IO.Iteratee[Set[RowsResultFlags]] =
    for {
      flags <- readInt
    } yield {
      val s = Set[RowsResultFlags]()
      if ((flags & 0x0001) != 0) {
        s += RowsResultFlags.GlobalTableSpec
      }
      s
    }

  def readKeyspaceSpec(flags: Set[RowsResultFlags]): IO.Iteratee[Option[KeyspaceSpec]] =
    TODO: This doesn't work when called in both places!
    if (flags contains RowsResultFlags.GlobalTableSpec) {
      for {
        keyspaceName <- readString
        tablename <- readString
      } yield Some(KeyspaceSpec(keyspaceName, tablename))
    } else {
      IO.Iteratee(None)
    }

  def readColumnType: Class[RowValue] = {
    for {
      columnType <- readShort
    }
  }
  
  @tailrec
  def readColumnSpecs(flags: Set[RowsResultFlags], columnsRemaining: Int, it: IO.Iteratee[List[ColumnSpec]]): IO.Iteratee[List[ColumnSpec]] = {
    if (columnsRemaining > 0) {
      val tmp = for {
        keyspaceSpec <- readKeyspaceSpec(flags)
        columnName <- readString
        columnType <- 
      } yield 
      readColumnSpecs(flags, columnsRemaining - 1, tmp)
    } else {
      it
    }
  }

  def readRowsResultMetadata: IO.Iteratee[RowsResultMetadata] =
    for {
      flags <- readRowsResultFlags
      columnsCount <- readInt
      globalTableSpec <- readGlobalTableSpec(flags)
      columSpec <- readColumnSpecs(flags, columnsCount, IO.Iteratee(List[ColumnSpec]()))
    } yield RowsResultMetadata(flags, globalTableSpec, columnsCount, columSpec)

  def readRowsResult(header: Header): IO.Iteratee[RowsResultMessage] =
    null

  def readSetKeyspaceResult(header: Header): IO.Iteratee[SetKeyspaceResultMessage] =
    for {
      keySpace <- readString
    } yield SetKeyspaceResultMessage(header, keySpace)

  def readPrepraedResultResult(header: Header): IO.Iteratee[PrepareResultMessage] =
    null

  def readSchemaChangeResult(header: Header): IO.Iteratee[SchemaChangeResultMessage] =
    for {
      change <- readString
      keyspace <- readString
      table <- readString
    } yield SchemaChangeResultMessage(header, change, keyspace, table)

  def readResultKind(flag: Short, header: Header): IO.Iteratee[ResultMessage] = {
    flag match {
      case 0x0001 => readVoidResult(header)
      case 0x0002 => readRowsResult(header)
      case 0x0003 => readSetKeyspaceResult(header)
      case 0x0004 => readPrepraedResultResult(header)
      case 0x0005 => readSchemaChangeResult(header)
    }
  }

  def readResultMessage(header: Header): IO.Iteratee[ResultMessage] =
    for {
      flag <- readShort
      m <- readResultKind(flag, header)
    } yield m

  def readMessageType(header: Header): IO.Iteratee[Message] = {
    header.opcode match {
      case Error => readErrorMessage(header)
      case Ready => readReadyMessage(header)
      case Authenticate => null
      case Supported => null
      case Result => readResultMessage(header)
      case Event => null
    }
  }

  def readMessage: IO.Iteratee[Message] =
    for {
      header <- readHeader
      bodyBytes <- IO take header.length
    } yield {
      val bodyIteratee = IO.IterateeRef.sync(readMessageType(header))
      bodyIteratee(IO Chunk bodyBytes)
      bodyIteratee.value._1.get
    }
}
