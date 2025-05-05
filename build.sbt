
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "Spark-DataSource-Arrow-Flight-SQL",
    javaOptions ++= Seq(
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-reads=org.apache.arrow.flight.core=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
    )
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.5" exclude("com.fasterxml.jackson.module", "jackson-module-scala"),
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.2",
  "org.apache.arrow" % "flight-sql" % "18.2.0"
)