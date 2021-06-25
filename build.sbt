name := "diplom-scala"

version := "0.1"

scalaVersion := "2.12.13"
val sparkVersion = "3.1.0"
val circeVersion = "0.14.0-M4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.hadoop" % "hadoop-client" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.2.5",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

"org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
  //"ch.qos.logback" % "logback-classic" % "1.2.3",
/*  "org.apache.commons" % "commons-csv" % "1.8",
  "io.circe" %% "circe-generic-extras" % "0.13.1-M4",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-literal" % circeVersion*/
)

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
//libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2" % Test
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % sparkVersion




