name := "hello"

version := "1.0"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

scalaVersion := "2.10.0"

resolvers += "spray repo" at "http://repo.spray.io"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "io.spray" % "spray-can" % "1.1-M7"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.1.0"

libraryDependencies += "com.typesafe.akka" %% "akka-osgi" % "2.1.0"

