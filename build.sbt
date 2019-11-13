name := "code"
version := "0.1"
scalaVersion := "2.12.10"

val sparkVersion = "2.4.4"
val argonautVersion = "6.2.2"
val scalaTestVersion = "3.0.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "io.argonaut" %% "argonaut" % argonautVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

test in assembly := {}