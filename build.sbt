name := "StreamFlixAnalytics"
version := "0.1"
scalaVersion := "2.12.15" // O 2.13 según la versión de Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0"
)

