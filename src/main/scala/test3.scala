package com.palmercox

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Failure
import scala.util.Success
import akka.actor.ActorSystem
import akka.util.ByteString
import akka.util.ByteStringBuilder
import java.nio.ByteOrder
import akka.actor.IO

import com.palmercox.casscala._
import com.palmercox.casscala.CassandraConnection.Message

object Test3 extends App {
  implicit val system = ActorSystem()
  val myConn = CassandraConnection("localhost", 9042)
  val result: Future[Message] = myConn.ping("")
  result onComplete {
    case Success(m) =>
      println("RECV: " + m)

    case Failure(f) =>
      println(f)
  }
  try {
    Await.result(result, 60 seconds)
  } finally {
    myConn.close()
    system.shutdown()
  }
}
