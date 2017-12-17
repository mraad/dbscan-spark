organization := "com.esri"

name := "dbscan-spark"

version := "0.3"

scalaVersion := "2.10.6"

publishMavenStyle := true

resolvers += Resolver.mavenLocal

sparkVersion := "1.6.2"

sparkComponents := Seq("core")

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

mainClass in assembly := Some("com.esri.dbscan.DBSCANApp")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.3" % "test"
)
