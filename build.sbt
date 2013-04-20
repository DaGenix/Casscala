name := "casscala"

organization := "com.palmercox"

version := "1.0"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

scalaVersion := "2.10.1"

resolvers ++= Seq(
	"spray repo" at "http://repo.spray.io",
	"spray nightly" at "http://nightlies.spray.io",
	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/")

libraryDependencies ++= Seq(
	"io.spray" % "spray-io" % "1.1-20130416",
	"io.spray" % "spray-can" % "1.1-20130416",
	"com.typesafe.akka" %% "akka-actor" % "2.1.0",
	"com.typesafe.akka" %% "akka-osgi" % "2.1.0",
	"org.scalaz" %% "scalaz-core" % "7.0.0-M9",
	"org.scalaz" %% "scalaz-effect" % "7.0.0-M9",
	"org.scalaz" %% "scalaz-iteratee" % "7.0.0-M9")
