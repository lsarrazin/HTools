name := "HBaseTools"
version := "0.1"

scalaVersion := "2.11.8"

val replVersion = "2.11.8"
val jlineVersion = "2.14.3"
val playVersion = "2.5.0" 
val testVersion = "3.0.1"

// Repl add-ons
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % replVersion,
  "org.scala-lang" % "scala-reflect" % replVersion,
  "jline" % "jline" % jlineVersion
)

// JSon parsing
libraryDependencies += "com.typesafe.play" %% "play-json" % playVersion 

// Hadoop add-ons
libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase" % "1.1.2",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "org.apache.hbase" % "hbase-client" % "1.1.2"
)

// Scala Tests
libraryDependencies += "org.scalactic" %% "scalactic" % testVersion
libraryDependencies += "org.scalatest" %% "scalatest" % testVersion % "test"


