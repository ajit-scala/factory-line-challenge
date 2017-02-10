name := """factory-line"""

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

// Uncomment to use Akka
//libraryDependencies ++= Seq(
//  "com.typesafe.akka" %% "akka-actor" % "2.4.16",
//  "com.typesafe.akka" %% "akka-stream" % "2.4.16"
//)

// Uncomment to use RX
libraryDependencies ++= Seq(
  "io.reactivex" %% "rxscala" % "0.26.5",
  "io.reactivex" % "rxjava" % "1.2.6"
)