name := "badgerq"

organization := "com.mosesn"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-core" % "6.8.1",
  "org.scalatest" %% "scalatest" % "2.0" % "test"
)
