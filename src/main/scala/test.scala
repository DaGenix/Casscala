package com.palmercox

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import com.palmercox.pooling._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import scala.util.Success
import spray.io.IOExtension
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Try
import akka.actor.IOManager

//object Hi extends App {
//  //  val actorSystem = ActorSystem()
//  //  val ioBridge = IOExtension(actorSystem).ioBridge()
//  //  val pool = actorSystem.actorOf(
//  //    Props(
//  //      new Pool(
//  //        TestConnectionFactory.createConnection(ioBridge, actorSystem))))
//  //
//  //  implicit val timeout = Timeout(5 seconds)
//  //
//  //  val futureConn = pool ? GetConnection()
//  //  val futureData: Future[String] = for {
//  //    conn <- futureConn.mapTo[Connection]
//  //    data <- conn.get
//  //  } yield data
//  //  futureData onComplete {
//  //    case Success(d) => { println(d); actorSystem.shutdown }
//  //    case Failure(f) => { println("FAILED: " + f); actorSystem.shutdown }
//  //  }
//
//  implicit val system = ActorSystem()
//  val myConn = MyConn("localhost", 8118)
//  for (i <- 1 to 10000) {
//    val result: Future[String] = myConn.ping("NUM: " + i)
//    result onComplete {
//      case Success(m) => { println("SENT: " + i + "; GOT: " + m) }
//      case Failure(f) => { println(f) }
//    }
//  }
//  Thread.sleep(6000)
//  system.shutdown()
//}
