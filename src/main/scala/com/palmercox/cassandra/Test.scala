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

object CassandraPool {
  sealed trait CassandraCommand
  final case class Bootstrap(hosts: Seq[InetSocketAddress])
  final case class Request(request: RequestMessage, tag: Any)
  final case object Shutdown

  sealed trait CassandraResult
  final case class Result(responseMessage: ResponseMessage, tag: Any) extends CassandraResult
  final case class Error(reason: String) extends CassandraResult

  private[cassandra] case class RequestInfo(requestor: ActorRef, num: Long, request: Request)
  private[cassandra] case class ResultInfo(inResponseTo: RequestInfo, responseMessage: ResponseMessage)
  private[cassandra] case class ChildConnected(child: ActorRef)
}

class CassandraConnectionActor(socketActor: ActorRef) extends Actor {
  import scala.collection.mutable
  import CassandraDefs._
  import CassandraMarshallingFuncs.marshall
  import CassandraPool._

  val unusedStreams = mutable.Set() ++ (0 to 126) // 127 reserved for the register message
  val pendingRequests = mutable.Map[Byte, RequestInfo]()

  override def preStart() = {
    socketActor ! Tcp.Register(self)
    context.watch(socketActor)

    socketActor ! Tcp.Write(marshall(0, StartupMessage(Map())))
  }

  val ref = IterateeRef.sync(repeat(for {
    r <- CassandraNativeParser.parser
  } yield {
    val header = r._1
    val response = r._2
    println("CHILD: " + response)
    response match {
      case ReadyMessage =>
        context.parent ! ChildConnected(self)

      case m: EventMessage =>
        context.parent ! m

      case m: ResponseMessage =>
        pendingRequests.remove(header.stream) match {
          case Some(ri: RequestInfo) =>
            unusedStreams += header.stream
            context.parent ! ResultInfo(ri, response)
          case None => println("Response message on unexpected stream!")
        }
    }
  }))

  def receive = {
    case r: RegisterMessage =>
      socketActor ! Tcp.Write(marshall(127, r))

    case ri @ RequestInfo(_, _, Request(req, _)) =>
      if (unusedStreams.size == 0) {
        // TODO - handle this better
        println("NO UNUSED STREAMS!")
      } else {
        val stream = unusedStreams.head.toByte
        unusedStreams -= stream
        pendingRequests += (stream -> ri)
        println("CHILD - SENDING REQUEST: " + marshall(stream, req))
        socketActor ! Tcp.Write(marshall(stream, req))
      }

    //
    // IO
    // 
    case Tcp.Received(data) =>
      println("CHILD - DATA: " + data)
      ref(Chunk(data))
    case _: Tcp.ConnectionClosed =>
      context.stop(self)

    //
    // Misc
    //
    case m => println("Unexpected Message: " + m)
  }
}

class CassandraPoolActor(val tcpActor: ActorRef) extends Actor {
  import CassandraPool._
  import scala.collection.mutable

  // Map of ActorRef of CassandraConnectionActor -> RequestInfo
  val inflightRequests = mutable.Map[ActorRef, Seq[RequestInfo]]()

  // List of all child actors that are managing connections
  val connActors = mutable.Set[ActorRef]()

  // List of all hosts that we know about
  val knownHosts = mutable.Set[InetSocketAddress]()

  // the child actor that is listening for gossip events
  var gossipActor: ActorRef = null

  var requestNum = 0L

  def receive = {
    //
    // Commands
    //
    case Bootstrap(hosts) =>
      this.knownHosts ++= hosts
      for (host <- hosts) {
        tcpActor ! Tcp.Connect(host)
      }

    case Shutdown =>
      // TODO - fail all pending requests
      context.stop(self)

    case r: Request =>
      if (connActors.size == 0) {
        sender ! new Exception("No connections!")
      } else {
        // TODO - do a better job of picking a child actor
        val child = connActors.head
        child ! RequestInfo(sender, requestNum, r)
        requestNum += 1
      }

    //
    // Response handling
    //
    case ResultInfo(ri, responseMessage) =>
      ri.requestor ! Result(responseMessage, ri.request.tag)

    //
    // IO related stuff
    //
    case Tcp.Connected(_, _) =>
      val s = sender
      val connAct = context.actorOf(Props(new CassandraConnectionActor(s)))
      context.watch(connAct)
    // Note: Not added to connActors until it completes connecting

    //
    // Child handling
    //
    case ChildConnected(childRef) =>
      connActors += childRef

      if (gossipActor == null) {
        gossipActor = childRef
        childRef ! RegisterMessage(Seq("TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"))
      }

    case Terminated(actor) =>
      // Complete all pending requests with an error
      connActors -= actor
      for (requests <- inflightRequests.remove(actor); ri <- requests) {
        // TODO - Better error!
        ri.requestor ! new Exception("Failed!")
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

  server ! Bootstrap(List(new InetSocketAddress("localhost", 9042)))

  Thread.sleep(2000)

  val test = akka.util.Timeout
  implicit val timeout = akka.util.Timeout(30 seconds)
  val result = server ? Request(QueryMessage("select * from system.schema_columns;", OneConsistency), None)

  result onComplete {
    r =>
      println("GOT: " + r)
  }

  System.in.read()
  system.shutdown()
}
