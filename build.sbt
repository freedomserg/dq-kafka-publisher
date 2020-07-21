name := "dq-kafka-gen"

version := "0.1"

scalaVersion := "2.11.12"

enablePlugins(AssemblyPlugin)

assemblyOutputPath in assembly := file("./dq-kafka-gen.jar")

libraryDependencies ++= Seq("org.apache.kafka"         %% "kafka"                      % "0.10.2.0")
