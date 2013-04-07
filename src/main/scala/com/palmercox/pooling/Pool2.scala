package com.palmercox.pooling

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

sealed case class PingMessage(val message: String)

class MyConn private (private val connActor: ActorRef)(
  implicit private val system: ActorSystem,
  private val timeout: Timeout = Timeout(60 seconds)) {

  def ping(message: String): Future[String] = {
    val f = connActor ? PingMessage(message)
    f.mapTo[String]
  }

  def close() {
    system.stop(connActor)
  }
}

object MyConn {
  def apply(host: String, port: Int)(implicit system: ActorSystem) = {
    val connActor = system.actorOf(Props(new MyConnActor(host, port)))
    new MyConn(connActor)
  }
}

sealed case class WaiterInfo(ref: IO.IterateeRefSync[String], requestor: ActorRef)

class MyConnActor(private val host: String, private val port: Int) extends Actor {
  private val socket: SocketHandle = IOManager(context.system).connect(host, port)

  private var closed = false

  private var waiters = Queue[WaiterInfo]()

  override def postStop = socket.close()

  private def ping(message: String) = {
    if (closed) {
      sender ! Status.Failure(new Exception("Conection is closed"))
    } else {
      waiters = waiters.enqueue(WaiterInfo(IO.IterateeRef.sync(MyConnActor.readMessage), sender))
      socket.write(ByteString(message, "UTF-8") ++ ByteString("\0", "UTF-8"))
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

object MyConnActor {
  private def utf8(bytes: ByteString): String = bytes.decodeString("UTF-8")

  private def readMessage: IO.Iteratee[String] = for {
    d <- IO takeUntil ByteString("\0")
  } yield utf8(d)
}
