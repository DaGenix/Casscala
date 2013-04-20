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
import scala.collection.mutable.ImmutableMapAdaptor
import akka.actor.FSM
import scala.util.Random
import scala.annotation.tailrec
import scala.concurrent.Promise

object CassandraPool {
  sealed trait Command
  final case class Execute(message: RequestMessage, tag: Any) extends Command

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

// bootstrap        - no connections; all requests queued
// picking gossiper - service requests, but just randomly. Figure out all child's tokens. execute a full remap() at transition out of state.
// running          - retry connections to all failed hosts every 30 seconds unless other nodes think the host is down
// stopped          - not running

object ChildTracker {
  import CassandraPool._
  import scala.collection.immutable._

  // Events
  case class RetryChild(address: InetSocketAddress)
  case class GossipSetupCompleted(activatingGossiper: ActorRef)

  // States
  // Bootstrap       - in this state, we have a set of possible hosts to connect to, but don't have any
  //                   connection yet. We keep re-trying every host every 30 seconds until we get a 
  //                   connection. When we get a connection (fully ready) we transition to the 
  //                   PickingGossiper state. Any requests that show up during this time are queued
  //                   and then issued as part of the transition to PickingGossiper.
  // PickingGossiper - In this state we register for gossip messages with one of our connections.
  //                   After the registration complete, we then retrieve a list of all the known
  //                   hosts, their status, and the tokens each services. Once we have all of that,
  //                   we transition to the Running state. During this time, any requests that show
  //                   up are just issued to a random connection. Any connections that fail are retried
  //                   every 30 seconds.
  // Running         - In this state we have a complete map of the full Cluster. Any failed connection
  //                   is retried every 30 seconds, unless its known to be down, in which case we
  //                   don't retry until its back up. If the gossiper fails, we go back to PickingGossiper
  //                   as long as we still have at least one connection left. If we have no connections left,
  //                   we go back to Bootstrap with whatever the list of known hosts looks like at the time.
  //                   Request that show up during this state are issued to the best connection if the
  //                   Request contains a hint.
  sealed trait State
  case object BootstrapState extends State
  case object PickingGossiperState extends State
  case object RunningState extends State

  // Data
  // knownHosts         - Set of all hosts - but we have no idea which are up and which are down
  // childHost          - a Map of children to the host they are connected (or trying to connect) to
  // readyChildren      - a Set of children that have connected and are ready to process queries
  // hostStats          - a Map that keeps track of each hosts status - up or down (true or false)
  // childTokens        - a Map that keeps track of which tokens each child has
  // tokens             - a Map that keeps track of which tokens are closest to each child
  // activatingGossiper - the child that was picked as the gossip actor that is starting up
  // gossiper           - the child that is servicing gossip requests
  sealed trait Data
  case class BootstrapData(
    knownHosts: Set[InetSocketAddress],
    childHost: Map[ActorRef, InetSocketAddress],
    queuedRequests: Seq[ExecuteInfo]) extends Data
  case class PickingGossiperData(
    knownHosts: Set[InetSocketAddress],
    childHost: Map[ActorRef, InetSocketAddress],
    readyChildren: Set[ActorRef],
    inflight: Map[ActorRef, Set[ExecuteInfo]],
    activatingGossiper: ActorRef) extends Data
  case class RunningData(
    knownHosts: Set[InetSocketAddress],
    hostTokens: Map[InetSocketAddress, Set[Long]],
    childHost: Map[ActorRef, InetSocketAddress],
    childTokens: Map[ActorRef, Set[Long]],
    tokens: SortedMap[Long, ActorRef],
    inflight: Map[ActorRef, Set[ExecuteInfo]],
    gossiper: ActorRef) extends Data
}

trait MessageSender { me: Actor =>
  import CassandraPool._
  import scala.collection.immutable._

  def sendMessage(
    inflight: Map[ActorRef, Set[ExecuteInfo]],
    child: ActorRef,
    execInfo: ExecuteInfo): Map[ActorRef, Set[ExecuteInfo]] = {

    child ! execInfo

    inflight + (child -> (inflight.getOrElse(child, Set()) + execInfo))
  }

  def delegate(
    exec: Execute): ExecuteInfo = {

    val requestor = sender
    def onSuccess(responseMessage: ResponseMessage) {
      requestor ! Completed(responseMessage, exec.tag)
    }
    def onFailure(reason: String) {
      requestor ! Error(reason, exec.tag)
    }

    ExecuteInfo(exec.message, onSuccess, onFailure)
  }

  def completeMessage(
    inflight: Map[ActorRef, Set[ExecuteInfo]],
    execInfo: ExecuteInfo,
    responseMessage: ResponseMessage): Map[ActorRef, Set[ExecuteInfo]] = {

    execInfo.onSuccess(responseMessage)

    inflight.get(sender) match {
      case Some(q) =>
        val newQ = q - execInfo
        if (newQ.isEmpty) inflight - sender else inflight + (sender -> newQ)
      case None => inflight
    }
  }

  def failChildMessages(inflight: Map[ActorRef, Set[ExecuteInfo]], child: ActorRef): Map[ActorRef, Set[ExecuteInfo]] = {
    for (q <- inflight.get(child); execInfo <- q) {
      execInfo.onFailure("Child failed")
    }
    inflight - child
  }

  def failAllMessages(inflight: Map[ActorRef, Set[ExecuteInfo]]): Map[ActorRef, Set[ExecuteInfo]] = {
    for (q <- inflight.values; execInfo <- q) {
      execInfo.onFailure("Child failed")
    }
    Map()
  }
}

class CassandraPoolActor(val tcpActor: ActorRef, initialHosts: Traversable[InetSocketAddress])
  extends Actor with MessageSender with FSM[ChildTracker.State, ChildTracker.Data] {

  import CassandraPool._

  import ChildTracker._
  import CassandraPool._
  import scala.collection.immutable._
  import scala.concurrent.duration._

  val rand = new Random()

  def randomChild(readyChildren: Set[ActorRef]): ActorRef = {
    // TODO - this feels innefficient
    readyChildren.toSeq(rand.nextInt(readyChildren.size))
  }

  def bestChild(tokens: SortedMap[Long, ActorRef], hint: Option[Long]): ActorRef = {
    hint match {
      case Some(h) =>
        val proj = tokens.to(h)
        if (proj.isEmpty) {
          tokens.last._2
        } else {
          proj.last._2
        }
      case None => bestChild(tokens, Some(rand.nextLong()))
    }
  }

  def createChild(host: InetSocketAddress): ActorRef = {
    val child = context.actorOf(Props(new CassandraConnectionActor(tcpActor, host)))
    context.watch(child)
    child
  }

  /**
   * Create child actors to connect to all the known hosts.
   */
  def bootstrapChildren(): Map[ActorRef, InetSocketAddress] = {
    Map() ++ (for { h <- initialHosts } yield {
      val child = createChild(h)
      (child -> h)
    })
  }

  startWith(BootstrapState, BootstrapData(
    knownHosts = Set() ++ initialHosts,
    childHost = bootstrapChildren(),
    queuedRequests = Seq()))

  when(BootstrapState) {
    case Event(exec: Execute, d: BootstrapData) =>
      stay using (d.copy(queuedRequests = d.queuedRequests :+ delegate(exec)))

    case Event(ChildReady, d: BootstrapData) =>
      val newChild = sender

      val newInflight = d.queuedRequests.foldRight[Map[ActorRef, Set[ExecuteInfo]]](Map()) { (execInfo, accum) =>
        sendMessage(accum, newChild, execInfo)
      }

      // TODO - kick off gossip setup queries

      goto(PickingGossiperState) using PickingGossiperData(
        knownHosts = d.knownHosts,
        childHost = d.childHost,
        readyChildren = Set(newChild),
        newInflight,
        activatingGossiper = newChild)

    case Event(Terminated(child), d: BootstrapData) =>
      for (host <- d.childHost.get(child)) {
        context.system.scheduler.scheduleOnce(30 seconds, self, RetryChild(host))
      }
      stay using (d.copy(childHost = d.childHost - child))

    case Event(RetryChild(host), d: BootstrapData) =>
      val newChild = createChild(host)
      stay using (d.copy(childHost = d.childHost + (newChild -> host)))

    case e =>
      println("UNHANDLED: " + e)
      stay
  }

  when(PickingGossiperState) {
    case Event(exec: Execute, d: PickingGossiperData) =>
      val newInflight = sendMessage(d.inflight, randomChild(d.readyChildren), delegate(exec))
      stay using d.copy(inflight = newInflight)

    case Event(ChildReady, d: PickingGossiperData) =>
      stay using d.copy(readyChildren = d.readyChildren + sender)

    case Event(Terminated(child), d: PickingGossiperData) =>
      for (host <- d.childHost.get(child)) {
        context.system.scheduler.scheduleOnce(30 seconds, self, RetryChild(host))
      }

      failChildMessages(d.inflight, child)

      val newReadyChilren = d.readyChildren - child
      if (newReadyChilren.isEmpty) {
        goto(BootstrapState) using (BootstrapData(
          knownHosts = d.knownHosts,
          childHost = d.childHost - child,
          queuedRequests = Seq()))
      } else {
        if (child == d.activatingGossiper) {
          stay using (d.copy(
            childHost = d.childHost - child,
            readyChildren = newReadyChilren,
            activatingGossiper = randomChild(newReadyChilren)))
        } else {
          stay using (d.copy(
            childHost = d.childHost - child,
            readyChildren = newReadyChilren))
        }
      }

    case Event(RetryChild(host), d: PickingGossiperData) =>
      val newChild = createChild(host)
      stay using (d.copy(childHost = d.childHost + (newChild -> host)))

    case Event(ret: ExecuteReturnInfo, d: PickingGossiperData) =>
      stay using (d.copy(inflight = completeMessage(d.inflight, ret.inResponseTo, ret.message)))

    case e =>
      println("UNHANDLED: " + e)
      stay
  }

  when(RunningState) {
    case _ => stay
  }

  whenUnhandled {
    case e =>
      println("UNHANDLED: " + e)
      stay
  }

  initialize
}

object Test extends App {
  import CassandraPool._
  import CassandraDefs._
  import scala.concurrent.duration._
  import akka.util.Timeout._

  implicit val system = ActorSystem()
  try {
    val tcpActor = IO(Tcp)
    val server = system.actorOf(Props(new CassandraPoolActor(tcpActor, Seq(new InetSocketAddress("127.0.1.1", 9042)))))

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
  } finally {
    System.in.read()
    system.shutdown()
  }
}
