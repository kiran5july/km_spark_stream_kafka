name := "KM Spark Kafka Project"
assemblyJarName := "km_sc_spark_strm_kafka.jar"
version := "0.1"

scalaVersion := "2.11.0"

resolvers ++= Seq(
  //DefaultMavenRepository
  "Artifactory" at "https://artifactory.allstate.com/artifactory/maven-central/"
  ,"Confluent" at "https://artifactory.allstate.com/artifactory/confluent-remote-cache/"
  ,"BintrayCache" at "https://artifactory.allstate.com/artifactory/jcenter-bintray-cache/"
  ,"allstate-roadside-services" at "https://artifactory.allstate.com/artifactory/allstate-roadside-services"
  //,Resolver.mavenLocal
)

val spark_v = "2.3.0"
//val scalaTest_v = "3.0.1"
//val scala_v = "2.11"


//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"
//dependencyOverrides += "io.netty" % "netty" % "3.10.5.Final"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_v //% "provided"
  ,"org.apache.spark" %% "spark-sql" % spark_v
  ,"org.apache.spark" %% "spark-streaming" % spark_v
  ,"org.apache.spark" %% "spark-hive" % spark_v
  ,"org.apache.spark" %% "spark-streaming-kafka-0-10" % spark_v exclude("org.slf4j","slf4j-log4j12") //slf4j error
  ,"org.apache.spark" %% "spark-sql-kafka-0-10" % spark_v
  ,"org.apache.avro"  %  "avro"  %  "1.3.3"
  ,"com.databricks" %% "spark-avro" % "4.0.0"
  ,"com.typesafe" % "config" % "0.3.1"
  //,"org.scalatest" %% "scalatest" % scalaTest_v % "test"
  //,"org.scalactic" %% "scalactic" % scalaTest_v
  ,"io.confluent" % "kafka-avro-serializer" % "4.0.0"
  ,"io.confluent" % "kafka-streams-avro-serde" % "4.0.0"
  //,"org.slf4j" % "slf4j-log4j12" % "1.7.22"
    //,"org.apache.kafka" % "kafka-clients" % "0.11.0.0"
  ,"com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
)
  //.map(_.exclude("org.slf4j", "*"))




/*
// ScalaTest
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
    exclude("org.scala-lang", "scala-reflect")
    exclude("org.scala-lang.modules", "scala-xml")
)
*/


assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "overview.html" => MergeStrategy.discard //KM
  //case PathList("net", "jpountz", xs@_*) => MergeStrategy.discard
  case PathList("org", "lz4", xs@_*) => MergeStrategy.discard //KM
  case "rootdoc.txt" => MergeStrategy.discard //KM
  case "git.properties" => MergeStrategy.discard //KM
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

/*
//Exclude Jars
assemblyExcludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {x =>
    x.data.getName.matches("lz4-java-1.4.0.jar") //|| x.data.getName.contains("spark-core")
  }
}
*/
