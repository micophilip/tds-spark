name := "code"
version := "0.1"
scalaVersion := "2.12.10"

val sparkVersion = "2.4.4"
val argonautVersion = "6.2.2"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.argonaut" %% "argonaut" % argonautVersion
)