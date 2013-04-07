//package com.palmercox.wordreader
//
//import akka.actor._
//import akka.util.{ ByteString, ByteStringBuilder }
//import akka.pattern.ask
//import scala.concurrent.Future
//import java.nio.ByteBuffer
//import spray.io.IOBridge
//import spray.util.ConnectionCloseReasons
//import akka.util.Timeout
//import scala.concurrent.Promise
//import scala.concurrent.duration._
//import scala.concurrent.ExecutionContext.Implicits.global
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
//  def gotData(buffer: ByteBuffer) {
//  }
//
//  def complete() {
//    promise success ""
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
//object WordReaderFactory {
//  implicit val timeout = Timeout(5 seconds)
//
//  def createConnection(ioBridge: ActorRef, actorSystem: ActorSystem)(): Future[Connection] = {
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
