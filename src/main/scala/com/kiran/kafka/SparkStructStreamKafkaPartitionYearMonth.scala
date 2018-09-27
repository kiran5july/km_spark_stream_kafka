package com.kiran.kafka


//import com.utils.VisionUtils.{VisionEvent}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, PrintWriter, StringWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.types.{FloatType, LongType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._
import scala.io.Source
import scala.reflect.ClassTag


object SparkStructStreamKafkaPartitionYearMonth {

  val logger = Logger.getLogger(this.getClass().getName())

  var statsSchema: Schema = _
  var valueDeserializer: KafkaAvroDeserializer = _

  var kafkaTopicName = //"rtalab.allstate.is.vision.ingest"
  //  "rtalab.allstate.is.vision.stats"
  //"rtalab.allstate.is.vision.alerts"
  //"rtalab.allstate.is.vision.test10" //events
    "rtalab.allstate.is.vision.alerts_kiran" //stats
  //"rtalab.allstate.is.vision.alerts_durga" //alerts

  def main(args: Array[String]) {

    logger.info("Class = " + this.getClass().getName())
    logger.info(getDT() + ": SparkStructStreamKafkaPartitionYearMonth start.")

    if(args.length > 0) {
      kafkaTopicName = args(0);
    }
    logger.info("Topic: " + kafkaTopicName)
    val bootstrapServers = "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092"
    //"localhost:9092" //"lxe0730.allstate.com:9092,lxe0731.allstate.com:9092,lxe0731.allstate.com:9092"

    val grpId_strm = "KM_TOPIC_Messages_0003"
    val schemaRegistryUrl = "http://lxe0961.allstate.com:8081"

    val conf = new SparkConf()
      .setAppName("Spark Struct Streaming data from Kafka")
      .setMaster("local[4]")
      .set("spark.io.compression.codec", "snappy") //I think default using lz4

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate

    spark.conf.getAll.mkString("\n").foreach(print)
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      import spark.sqlContext.implicits._
      val dataDF = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("subscribe", kafkaTopicName)
        .option("startingOffsets", "earliest") //Can't be latest for batch queries
        .option("failOnDataLoss", "false")
        .load
        .mapPartitions(currentPartition => {

          currentPartition.map(rec => {
            val timestamp = rec.getAs[Timestamp]("timestamp")
            (timestamp)
          })
        })
        //.toDF("topic", "partition", "timestamp", "value")
        .toDF("timestamp")

      //println("*** DF cache & Rec count= "+statsData.count()) //1813
      //statsData.show(2000, false)

      if(dataDF.head(1).length <= 0) {
        logger.error("*** Alert! Missing stats/reference data. ***")
        println("*** Alert! Missing stats/reference data. ***")
      }else {

        import org.apache.spark.sql.functions._
        val yrMonths = dataDF
          //.withColumn("timestamp_DT", (col("timestamp").cast(LongType)/1000).cast(TimestampType)) //NOT NEEDED
          .withColumn("timestamp_year", year($"timestamp"))
          .withColumn("timestamp_month", month($"timestamp"))
          .drop("timestamp")
          //.drop("partition")
          //.groupBy($"timestamp_year",$"timestamp_month")
          .dropDuplicates("timestamp_year", "timestamp_month")

        println("*** Year/Months count= " + yrMonths.count() + " ***")
        yrMonths.show(20, false)

        println("*** Topic messages printed.")
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        //logger.error("Error occurred while getting latest stats data and broadcasting it. \n" + sw.toString)
        println("Error occurred while getting data from topic. \n" + sw.toString)
    }

  }

  @throws(classOf[Exception])
  def getRESTSchemaBySubject(schemaURL: String, subjectName: String) : org.apache.avro.Schema = {

    Try(new RestService(schemaURL).getLatestVersion(subjectName).getSchema) match {
      case Success(s) => {
        logger.info(s"Found schema for $subjectName")
        new Schema.Parser().parse(s)
      }
      case Failure(f) => {
        logger.warn("Unable to connect to the Schema registry. An attempt will be made to create the table" +
          " on receipt of the first records.")
        null
      }
    }

  }


  def getDT()
  : String = {

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }

  def getType[T](v: T) = v

}

