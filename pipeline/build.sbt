val scala3Version = "2.12.18"
val sparkVersion = "3.5.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Pipeline",
    version := "0.1.0",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,

    // Spark
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.4",
    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
    libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",
    libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0"
    // libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.4.0"
  )
fork := true

javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
)