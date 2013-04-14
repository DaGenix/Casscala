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
  final case class Error(reason: String, tag: Any) extends CassandraResult

  private[cassandra] case class RequestInfo(requestor: ActorRef, num: Long, request: Request)
  private[cassandra] case class ResultInfo(inResponseTo: RequestInfo, responseMessage: ResponseMessage)
  private[cassandra] case class ChildConnected(child: ActorRef)
}

class CassandraConnectionActor(tcpActor: ActorRef, addr: InetSocketAddress) extends Actor {
  import scala.collection.mutable
  import CassandraDefs._
  import CassandraMarshallingFuncs.marshall
  import CassandraPool._

  type StreamId = Byte

  val unusedStreams = mutable.Set() ++ (0 to 127)

  val pendingRequests = mutable.Map[StreamId, RequestInfo]()

  var socketActor: ActorRef = null

  override def preStart() = {
    tcpActor ! Tcp.Connect(addr)
  }

  val ref = IterateeRef.sync(repeat(for {
    r <- CassandraNativeParser.parser
  } yield {
    val header = r._1
    val response = r._2

    unusedStreams += header.stream

    println("CHILD: " + response)
    response match {
      case ReadyMessage =>
        context.parent ! ChildConnected(self)

      case m: EventMessage =>
        context.parent ! m

      case m: ResponseMessage =>
        pendingRequests.remove(header.stream) match {
          case Some(ri: RequestInfo) =>
            context.parent ! ResultInfo(ri, response)
          case None => println("Response message on unexpected stream!")
        }
    }
  }))

  def send(message: RequestMessage): StreamId = {
    val stream = unusedStreams.head.toByte
    unusedStreams -= stream
    socketActor ! Tcp.Write(marshall(stream, message))
    stream
  }

  def receive = {
    case r: RegisterMessage =>
      send(r)

    case ri @ RequestInfo(_, _, Request(req, _)) =>
      if (unusedStreams.size == 0) {
        // TODO - handle this better
        println("NO UNUSED STREAMS!")
      } else {
        val stream = send(req)
        pendingRequests += (stream -> ri)
      }

    //
    // IO
    //
    case _: Tcp.Connected =>
      socketActor = sender
      socketActor ! Tcp.Register(self)
      context.watch(socketActor)
      send(StartupMessage(Map()))
    case Tcp.Received(data) =>
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
  val inflightRequests = mutable.Map[ActorRef, mutable.Set[RequestInfo]]()

  // Any requests that show up before the first child has finished connecting
  // get queued here until that child connects
  val queuedRequests = mutable.ListBuffer[RequestInfo]()

  // List of all child actors that are managing connections
  val connActors = mutable.Set[ActorRef]()

  // List of all child actors that are trying to connect
  val pendingConnActors = mutable.Set[ActorRef]()

  // List of all hosts that we know about
  val knownHosts = mutable.Set[InetSocketAddress]()

  // the child actor that is listening for gossip events
  var gossipActor: ActorRef = null

  var requestNum = 0L

  def handleChildReady(childRef: ActorRef) {
    connActors += childRef
    pendingConnActors -= childRef

    if (gossipActor == null) {
      gossipActor = childRef
      childRef ! RegisterMessage(Seq("TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"))
    }
  }

  def receive = startup

  def startup: Receive = {
    //
    // Commands
    //
    case Bootstrap(hosts) =>
      this.knownHosts ++= hosts
      for (host <- hosts) {
        val connAct = context.actorOf(Props(new CassandraConnectionActor(tcpActor, host)))
        context.watch(connAct)
      }

    // TODO - what about Shutdown?

    case r: Request =>
      queuedRequests += RequestInfo(sender, requestNum, r)
      requestNum += 1

    //
    // Child handling
    //
    case ChildConnected(childRef) =>
      handleChildReady(childRef)
      for (r <- queuedRequests) {
        val child = connActors.head
        child ! r
      }
      queuedRequests.clear()
      context.become(running)

    case m => println("Unexpected Message: " + m)
  }

  def running: Receive = {
    case Shutdown =>
      for (ri <- queuedRequests) {
        ri.requestor ! Error("Shutdown!", ri.request.tag)
      }
      context.stop(self)

    case r: Request =>
      if (connActors.size == 0) {
        sender ! Error("No connections!", r.tag)
      } else {
        val child = connActors.head
        child ! RequestInfo(sender, requestNum, r)
        requestNum += 1
      }

    //
    // Response handling
    //
    case ResultInfo(ri, responseMessage) =>
      for (ifr <- inflightRequests.get(sender)) {
        ifr -= ri
      }
      ri.requestor ! Result(responseMessage, ri.request.tag)

    //
    // Child handling
    //
    case ChildConnected(childRef) =>
      handleChildReady(childRef)

    case Terminated(actor) =>
      // Complete all pending requests with an error
      connActors -= actor
      for (requests <- inflightRequests.remove(actor); ri <- requests) {
        // TODO - Better error!
        ri.requestor ! Error("Child actor Failed!", ri.request.tag)
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

  implicit val timeout = akka.util.Timeout(30 seconds)
  val result = server ? Request(QueryMessage("select * from system.schema_columns;", OneConsistency), None)

  result onComplete { r =>
    println("GOT: " + r)
  }

  System.in.read()
  system.shutdown()
}
