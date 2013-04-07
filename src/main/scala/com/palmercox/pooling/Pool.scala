//package com.palmercox.pooling

//import akka.osgi.a

//
//import java.util.Date
//import scala.collection.mutable.ListBuffer
//import scala.collection.mutable.Queue
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration.DurationInt
//import scala.concurrent.duration.FiniteDuration
//import akka.actor.Actor
//import akka.actor.ActorRef
//import akka.pattern.{ ask, pipe }
//import scala.collection.mutable.HashSet
//import akka.util.Timeout
//import scala.util.Success
//import scala.util.Failure
//import scala.util.Failure
//import spray.io._
//import akka.actor.Props
//import akka.actor.ActorSystem
//import scala.concurrent.Promise
//import scala.concurrent.Future
//import scala.concurrent.duration.Duration
//import java.nio.ByteBuffer
//import scala.collection.immutable.List
//import scala.collection.mutable.StringBuilder
//import java.nio.charset.Charset
//import scala.io.Codec
//import spray.util.ConnectionCloseReasons
//
//trait Connection {
//  def get: Future[String]
//}
//
//class TestConnection(val readFuture: Future[String]) extends Connection {
//  def get: Future[String] = readFuture
//}
//
//class TestConnectionHandlerActor(val promise: Promise[String]) extends Actor {
//  val decoder = Charset.forName("ISO-8859-1").newDecoder()
//  val receivedData = ByteBuffer.allocate(1024)
//
//  def gotData(buffer: ByteBuffer) {
//    receivedData.put(buffer)
//  }
//
//  def complete() {
//    receivedData.flip()
//    val result = decoder.decode(receivedData).toString()
//    promise success result
//  }
//
//  def receive = {
//    case IOBridge.Received(handle, buffer) => gotData(buffer)
//    case IOBridge.Closed(handle, reason) => {
//      import ConnectionCloseReasons._
//      reason match {
//        case CleanClose | ConfirmedClose | PeerClosed => complete()
//        case ProtocolError(reason) => promise failure new Exception(reason)
//        case IOError(throwable) => promise failure throwable
//        case x => promise failure new Exception(x.getClass.toString)
//      }
//    }
//  }
//}
//
//object TestConnectionFactory {
//  implicit val timeout = Timeout(5 seconds)
//
//  def createConnection(ioBridge: ActorRef, actorSystem: ActorSystem)(): Future[Connection] = {
//    val od  =  Some(1)
//    val connect = ioBridge ? IOBridge.Connect("localhost", 8888)
//    val promise = Promise[String]
//    connect.map {
//      case IOBridge.Connected(theKey, theTag) => {
//        val theHandler = actorSystem.actorOf(Props(new TestConnectionHandlerActor(promise)))
//        ioBridge ! IOBridge.Register(new IOBridge.Handle() {
//          def key = theKey
//          def handler = theHandler
//          def tag = theTag
//        })
//        new TestConnection(promise.future)
//      }
//    }
//  }
//}
//
//case class GetConnection()
//case class PutConnection(val connection: ActorRef)
//
//class Pool(
//  val createConection: () => Future[Connection]) extends Actor {
//
//  case class CheckPool
//
//  case class PooledConnectionInfo(
//    val connection: ActorRef,
//    var lastUsedTime: Date)
//  case class InUseConnectionInfo(val connection: ActorRef)
//
//  val pooledConnections = HashSet[PooledConnectionInfo]()
//  val inUseConnections = HashSet[InUseConnectionInfo]()
//
//  var pendingConnections = 0
//
//  override def preStart() {
//    context.system.scheduler.schedule(0 seconds, 30 seconds, self, CheckPool)
//  }
//
//  def checkPool {
//    pooledConnections foreach { x =>
//      pooledConnections -= x
//    }
//  }
//
//  def getConnection = {
//    val requestor = sender
//    createConection() onComplete {
//      case Success(c) => requestor ! c
//      case Failure(f) => requestor ! akka.actor.Status.Failure(f)
//    }
//  }
//
//  def putConnection(connection: ActorRef) {
//    pooledConnections += PooledConnectionInfo(connection, new Date())
//  }
//
//  def receive = {
//    case CheckPool() => checkPool
//    case GetConnection() => getConnection
//    case PutConnection(connection) => putConnection(connection)
//  }
//}
