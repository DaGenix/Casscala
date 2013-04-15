package com.palmercox.cassandra

import java.net.InetSocketAddress
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import CassandraDefs.EventMessage
import CassandraDefs.OneConsistency
import CassandraDefs.QueryMessage
import CassandraDefs.ReadyMessage
import CassandraDefs.RegisterMessage
import CassandraDefs.RequestMessage
import CassandraDefs.ResponseMessage
import CassandraDefs.StartupMessage
import CassandraMarshallingFuncs.marshall
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.IO.Chunk
import akka.actor.IO.IterateeRef
import akka.actor.IO.repeat
import akka.actor.Props
import akka.actor.Terminated
import akka.actor.actorRef2Scala
import akka.io.IO
import akka.io.Tcp
import akka.pattern.ask
import scala.util.Success

object CassandraPool {
  sealed trait Command
  final case class Bootstrap(hosts: Traversable[InetSocketAddress]) extends Command
  final case class Execute(message: RequestMessage, tag: Any) extends Command
  final case object Shutdown extends Command

  sealed trait CommandResult
  final case class Completed(message: ResponseMessage, tag: Any) extends CommandResult
  final case class Error(reason: String, tag: Any) extends CommandResult

  private[cassandra] case class ExecuteInfo(message: RequestMessage, onSuccess: ResponseMessage => Unit, onFailure: String => Unit) {
    // Override these methods since we don't want to use structural equality
    override def hashCode(): Int = System.identityHashCode(this)
    override def equals(other: Any): Boolean = this == other
  }
  private[cassandra] case class ExecuteReturnInfo(inResponseTo: ExecuteInfo, message: ResponseMessage)

  private[cassandra] case object ChildReady
}

class CassandraConnectionActor(tcpActor: ActorRef, addr: InetSocketAddress) extends Actor {
  import scala.collection.mutable
  import CassandraDefs._
  import CassandraMarshallingFuncs.marshall
  import CassandraPool._

  type StreamId = Byte

  // Set of all streams that don't currently having pending requests
  val unusedStreams = mutable.Set() ++ (0 to 127).map { _.toByte }

  // Executions that have been submitted but haven't yet gotten responses
  val pending = mutable.Map[StreamId, ResponseMessage => Unit]()

  // ExecuteInfos that couldn't be sent since there were no unused streams
  // TODO - develope a mechanism to inform the parent that this connection is overwelmed
  val queued = mutable.ListBuffer[ExecuteInfo]()

  var socketActor: ActorRef = null

  override def preStart() = {
    tcpActor ! Tcp.Connect(addr)
  }

  val ref = IterateeRef.sync(repeat(for {
    x <- CassandraNativeParser.parser
  } yield {
    val (header, responseMessage) = x

    if (header.stream < 0) {
      context.parent ! responseMessage
    } else {
      unusedStreams += header.stream

      pending.remove(header.stream) match {
        case Some(completionHandler) => completionHandler(responseMessage)
        case None => println("Response message on unexpected stream!")
      }

      if (!queued.isEmpty) {
        delegateSend(queued.remove(0))
      }
    }
  }))

  def sendMessage(message: RequestMessage)(f: ResponseMessage => Unit): Boolean = {
    if (unusedStreams.isEmpty) {
      false
    } else {
      val stream = unusedStreams.head
      unusedStreams -= stream
      socketActor ! Tcp.Write(marshall(stream, message))
      pending += (stream -> f)
      true
    }
  }

  def delegateSend(execInfo: ExecuteInfo) {
    val sent = sendMessage(execInfo.message) { responseMessage =>
      context.parent ! ExecuteReturnInfo(execInfo, responseMessage)
    }
    if (!sent) {
      queued += execInfo
    }
  }

  def receive = {
    //
    // Commands
    //
    case execInfo: ExecuteInfo =>
      delegateSend(execInfo)

    //
    // IO
    //
    case _: Tcp.Connected =>
      socketActor = sender
      socketActor ! Tcp.Register(self)
      context.watch(socketActor)
      sendMessage(StartupMessage(Map())) { _ =>
        context.parent ! ChildReady
      }
    case Tcp.CommandFailed(cmd) =>
      context.stop(self)
    case _: Tcp.ConnectionClosed =>
      context.stop(self)
    case Tcp.Received(data) =>
      ref(Chunk(data))

    //
    // Misc
    //
    case m => println("Unexpected Message: " + m)
  }
}

class CassandraPoolActor(val tcpActor: ActorRef) extends Actor {
  import CassandraPool._
  import scala.collection.mutable

  // Map of all Execute commands currently being serviced by child actors
  val inflight = mutable.Map[ActorRef, mutable.Set[ExecuteInfo]]()

  // List of all child actors that are trying to connect
  val pendingConnActors = mutable.Set[ActorRef]()

  // List of all child actors that are managing connections
  // TODO - add in information about the position(s) in the ring
  val connActors = mutable.Set[ActorRef]()

  // List of all hosts that we know about
  val knownHosts = mutable.Set[InetSocketAddress]()

  // the child actor that is listening for gossip events
  var gossipActor: ActorRef = null

  def receive = {
    //
    // Commands
    //
    case Bootstrap(hosts) =>
      this.knownHosts ++= hosts
      for (host <- hosts) {
        val childRef = context.actorOf(Props(() => new CassandraConnectionActor(tcpActor, host)))
        pendingConnActors += childRef
        context.watch(childRef)
      }

    case Shutdown =>
      for (q <- inflight.values; execInfo <- q) {
        execInfo.onFailure("Shutdown")
      }
      context.stop(self)

    case Execute(message, tag) =>
      if (connActors.size == 0) {
        // TODO - queue up commands sent during bootstrap
        sender ! Error("No connections!", tag)
      } else {
        val requestor = sender
        val child = connActors.head

        def onSuccess(responseMessage: ResponseMessage) {
          requestor ! Completed(responseMessage, tag)
        }
        def onFailure(reason: String) {
          requestor ! Error(reason, tag)
        }

        child ! ExecuteInfo(message, onSuccess, onFailure)
      }

    //
    // Response handling
    //
    case ExecuteReturnInfo(execInfo, responseMessage) =>
      for (s <- inflight.get(sender)) {
        s -= execInfo
      }
      execInfo.onSuccess(responseMessage)

    //
    // Gossip protocol handling
    //
    case eventMessage: EventMessage =>

    //
    // Child handling
    //
    case ChildReady =>
      connActors += sender
      pendingConnActors -= sender
      if (gossipActor == null) {
        gossipActor = sender
        val requestMessage = RegisterMessage(Seq("TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"))
        sender ! ExecuteInfo(requestMessage, _ => (), _ => ())
      }

    case Terminated(actor) =>
      // Complete all pending requests with an error
      connActors -= actor
      pendingConnActors -= actor
      for (requests <- inflight.remove(actor); execInfo <- requests) {
        execInfo.onFailure("Child actor failed.")
      }

      if (gossipActor == actor) {
        gossipActor = null
        // TODO - pick and then register another connection
      }
  }
}

object Test extends App {
  import CassandraPool._
  import CassandraDefs._
  import scala.concurrent.duration._
  import akka.util.Timeout._

  implicit val system = ActorSystem()
  val tcpActor = IO(Tcp)
  val server = system.actorOf(Props(new CassandraPoolActor(tcpActor)))

  server ! Bootstrap(Seq(new InetSocketAddress("localhost", 9042)))

  Thread.sleep(500)

  implicit val timeout = akka.util.Timeout(30 seconds)
  val result = server ? Execute(QueryMessage("select key, tokens from system.local;", OneConsistency), None)

  result onComplete { r =>
    r match {
      case Success(Completed(RowsResultMessage(rows), _)) =>
        for (row <- rows) {
          //          row match {
          //            case Seq(keyspace_name, columnfamily_name, column_name, component_index, index_name, index_options, index_type, validator) =>
          //              println(s"$keyspace_name")
          //          }
          println(row)
        }
    }
  }

  System.in.read()
  system.shutdown()
}
