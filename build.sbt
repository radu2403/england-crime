name := "england-crime"

version := "0.1"


scalaVersion := "2.12.10"
val AkkaHttpVersion = "10.2.1"
val AkkaVersion = "2.6.8"
val circeVersion = "0.12.3"
val akkaHttp = "10.1.1"
val akka = "2.5.11"
val circe = "0.9.3"
val macwire = "2.3.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",

  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.0",
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",

  "com.typesafe.akka" %% "akka-http" % akkaHttp,
  "com.typesafe.akka" %% "akka-stream" % akka,
  "com.typesafe.akka" %% "akka-slf4j" % akka,

  "de.heikoseeberger" %% "akka-http-circe" % "1.20.1",

  "io.circe" %% "circe-generic" % circe,

  "com.softwaremill.macwire" %% "macros" % macwire,
  "com.softwaremill.macwire" %% "util" % macwire,

  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0",

  //test libraries
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.pegdown" % "pegdown" % "1.6.0" % "test",
  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttp % "test",
  "org.mongodb" % "mongo-java-driver" % "3.4.2",
  "com.github.fakemongo" % "fongo" % "2.1.0" % "test"

//  "com.typesafe.akka" %% "akka-actor"               % AkkaVersion,
//  "com.typesafe.akka" %% "akka-actor-typed"         % AkkaVersion,
//  "com.typesafe.akka" %% "akka-http"                % AkkaHttpVersion,
//  "com.typesafe.akka" %% "akka-http-spray-json"     % AkkaHttpVersion,
//  "com.typesafe.akka" %% "akka-actor-typed"         % AkkaVersion,
//  "com.typesafe.akka" %% "akka-stream"              % AkkaVersion,
//
//  "io.circe" %% "circe-core"     % circeVersion,
//  "io.circe" %% "circe-generic"  % circeVersion,
//  "io.circe" %% "circe-parser"   % circeVersion,
//
//  "de.heikoseeberger" %% "akka-http-circe" % "1.31.0",
//
//  "org.scalatest" %% "scalatest" % "3.2.2" % "test"
)