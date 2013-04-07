package com.palmercox

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Failure
import scala.util.Success
import com.palmercox.pooling.MyConn
import akka.actor.ActorSystem
import akka.util.ByteString
import akka.util.ByteStringBuilder
import java.nio.ByteOrder
import com.palmercox.casscala.CassandraConnection
import akka.actor.IO

object Test2 extends App {
  //  implicit val system = ActorSystem()
  //  val myConn = MyConn("localhost", 8118)
  //  var l = List[Future[String]]()
  //  for (i <- 1 to 30) {
  //    val result: Future[String] = myConn.ping("NUM: " + i)
  //    result onComplete {
  //      case Success(m) =>
  //        println("SENT: " + i + "; GOT: " + m)
  //
  //      case Failure(f) =>
  //        println(f)
  //    }
  //    l = result :: l
  //  }
  //  try {
  //    for (f <- l) { Await.result(f, 60 seconds) }
  //  } finally {
  //    myConn.close()
  //    system.shutdown()
  //  }

  private implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val i = new ByteStringBuilder()
  i.putShort(4)
  i.putShort(4)
  i.putBytes("tes1".getBytes)
  i.putShort(6)
  i.putBytes("test22".getBytes)
  i.putShort(4)
  i.putBytes("hih3".getBytes)
  i.putShort(6)
  i.putBytes("palme4".getBytes)
  val b = i.result

  println(b)

  //  val i2 = new ByteStringBuilder()
  //  i2.putShort(5)
  //  i2.putBytes("test5".getBytes("UTF-8"))
  //  val b2 = i2.result
  // println(b2)

  val it = IO.IterateeRef.sync(CassandraConnection.readStringList)
  it(IO Chunk b)

  println(it.value._1.get)
}
