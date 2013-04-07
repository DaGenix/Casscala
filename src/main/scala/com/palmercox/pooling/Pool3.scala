package com.palmercox.pooling.cassandra

import scala.collection.mutable.Queue
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
import scala.collection.mutable.Set
import scala.collection.mutable.HashSet

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

sealed case class Waiter(ref: IO.IterateeRefSync[String], r: ActorRef)

class MyConnActor(private val host: String, private val port: Int) extends Actor {
  private val socket: SocketHandle = IOManager(context.system).connect(host, port)

  private var closed = false

  private val availableStreamids = new HashSet[Int]()
  availableStreamids ++= (1 to 127)
  
  private val waiters = new Queue[Waiter]()

  override def postStop = socket.close()

  private def ping(message: String) = {
    if (closed) {
      sender ! Status.Failure(new Exception("Closed"))
    } else {
      waiters.enqueue(Waiter(IO.IterateeRef.sync(MyConnActor.readMessage), sender))
      socket.write(ByteString(message, "UTF-8") ++ ByteString("\0", "UTF-8"))
    }
  }

  private def recv(bytes: ByteString) {
    waiters.front.ref(IO Chunk bytes)
    while (waiters.size > 0) {
      val w = waiters.front
      w.ref.value match {
        case (IO.Done(v), i) => {
          w.r ! v
          waiters.dequeue
          if (waiters.size > 0) {
            waiters.front.ref(i)
          }
        }
        case (IO.Next(_), _) => return
        case (IO.Failure(f), i) => {
          for (w <- waiters) {
            w.r ! Status.Failure(f)
          }
          waiters.clear()
          closed = true
          return
        }
      }
    }
  }

  private def closed(cause: IO.Input) {
    closed = true
    for (w <- waiters) {
      w.r ! Status.Failure(new Exception("Connection Closed"))
    }
    waiters.clear()
  }

  def receive = {
    case PingMessage(message) => ping(message)
    case IO.Read(socket, bytes) => recv(bytes)
    case IO.Closed(socket, cause) => closed(cause)
    case IO.Connected(socket, address) =>
    case e => { println(e); ??? }
  }
}

object MyConnActor {
  private def utf8(bytes: ByteString): String = bytes.decodeString("UTF-8")

  private def readMessage: IO.Iteratee[String] = for {
    d <- IO takeUntil ByteString("\0")
  } yield utf8(d)
}
