name := "TS-RDD"

version := "1.0"

scalaVersion := "2.11.8"

exportJars := true

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.1" % "provided"
  ,"org.scala-lang" % "scala-library" % "2.11.8" % "provided"
  ,"org.scalatest" % "scalatest_2.11" % "2.2.6" % "test"
)
