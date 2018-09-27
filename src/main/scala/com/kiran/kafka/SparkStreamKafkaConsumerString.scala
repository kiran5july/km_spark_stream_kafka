package com.kiran.kafka

//import com.utils.VisionUtils.{VisionEvent}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.text.SimpleDateFormat
import java.util.Date

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
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, TimestampType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}


object SparkStreamKafkaConsumerString {
  val logger = Logger.getLogger(this.getClass().getName())
  var timeNowMS = System.currentTimeMillis()
  var timePrevMS = System.currentTimeMillis()
  var sSchema: Schema = _
  //var valueDeserializerX: KafkaAvroDeserializer = _

  def main(args: Array[String]) {
    println("Class = " + this.getClass().getName())
    logger.info(getDT()+": SparkStreamKafkaTopic start.")

    val bootstrapServers = "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092"
                          //"localhost:9092" //"lxe0730.allstate.com:9092,lxe0731.allstate.com:9092,lxe0731.allstate.com:9092"

    val kafkaTopic = //"rtalab.allstate.is.vision.ingest"
    //"rtalab.allstate.is.vision.stats"
    //"rtalab.allstate.is.vision.alerts"
    //"rtalab.allstate.is.vision.test" //DATA (null key, val)
    //"rtalab.allstate.is.vision.results_spark" //NO DATA
      //"rtalab.allstate.is.vision.test10" //events
      //"rtalab.allstate.is.vision.alerts_kiran" //stats
    "rtalab.allstate.is.vision.alerts_durga" //alerts
    //"rtalab.allstate.is.vision.results_str_spark" //NO DATA
                          //"km.vision.events.topic" // Comma separated list of topics

    val grpId_strm = "Kafka_Stream_KM_READER_String_02"
    val streamInterSec = 10 // in seconds
    //val refRefreshInterMin = 1 // 30 minutes
    val schemaRegistryUrl = "http://lxe0961.allstate.com:8081"


    val kafkaParamsEvt : Map[String, Object] = Map[String, Object](
      "group.id" -> System.currentTimeMillis().toString(), //grpId_strm
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest", //latest, earliest, none
      //"schema.registry.url" -> schemaRegistryUrl,
      "enable.auto.commit" -> (false: java.lang.Boolean)
      ,"sasl.kerberos.service.name" ->  "kafka"
      ,"security.protocol" -> "SASL_PLAINTEXT"
      ,"failOnDataLoss" -> (false: java.lang.Boolean)
    )

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Consume_Kafka_Events_KM")
      .set("spark.io.compression.codec", "snappy")
      //.set("spark.streaming.kafka.consumer.poll.ms", "2048")

    val spark = SparkSession.builder
      .config(conf)
      //.enableHiveSupport()
      .getOrCreate
/*
    //Using SparkSession & SparkContext
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Spark Streaming data from Kafka")
      //.enableHiveSupport()
      .getOrCreate()
*/
    //spark.conf.getAll.mkString("\n").foreach(print)
    val sc = spark.sparkContext

    sc.setLogLevel("INFO")

    val ssc = new StreamingContext(sc, Seconds(streamInterSec))
    //StreamingContext without SparkContext
    //val ssc = new StreamingContext("local[2]", "Spark Streaming data from Kafka", Seconds(30))

    println(getDT() + ": Building stream..")
    val stream =
      KafkaUtils.createDirectStream[String, String](
        //KafkaUtils.createDirectStream[String, String](
        //KafkaUtils.createDirectStream[String, KafkaAvroDeserializer](
        ssc,
        PreferConsistent,
        Subscribe[String, String](kafkaTopic.split(","), kafkaParamsEvt)
      ) //.map(_.value.toString)


    timeNowMS = System.currentTimeMillis()

    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._

    //process each RDD
    println(getDT() + ": Processing stream..")
    stream.foreachRDD( (rdd) => {
      //(rdd, time: Time) => {
        //println(getDT() + ": foreachRDD...")

        //if (!rdd.isEmpty()) {
          //val commitOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //println(getDT() + ": rdd is NOT empty; rows= " + rdd.count())  //rows= 27864
          try {

            //rdd.foreachPartition(itr => {


            //Option#1: WORKS; print as this
              val dfEvents = rdd.map( cr => (cr.key, cr.value().toString(), cr.timestamp(), cr.topic()))
                  //.filter(x => x._1 == 41619)
                //.foreach(println)
                  .toDF("key", "val", "timestamp", "topic")
                  .withColumn("timestamp_DT", (col("timestamp").cast(FloatType)/1000).cast(TimestampType))
                  //.filter($"timestamp_DT" >= "2018-05-31 00:00:00") // && $"timestamp_DT" >= "2018-05-30 20:00:00")

              //println("---> Records count="+ dfEvents.count())
              //dfEvents.printSchema()

              //if(dfEvents.count() > 0 ){ //NOT WORKING

                dfEvents
                  //.filter($"key" =!= null)
                    //.filter($"key" === "252495")
                  .show(20000, false)
              //}else{
              //  println("---> No Records.")
              //}



        }catch{
          case e: Exception => println("*** Exception.. Continuing to next RDD...")
              e.printStackTrace()
        }

            //Push the result to the Sink Kafka
            //stream.asInstanceOf[CanCommitOffsets].commitAsync(commitOffsetRanges)

          //})
        //} else {
        //  println(getDT() + ": No data received.")
          //logger.error(getDT() + ": No data received")
        //}
        //println(getDT() + ": Completed.")
        timePrevMS = timeNowMS
        timeNowMS = System.currentTimeMillis()
        println(getDT() + ": Kafka poll Started@ ("+timePrevMS+ "): " + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(timePrevMS)) +
          "  -->  ("+timeNowMS+"): " + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(timeNowMS)) +
          " = " + ((timeNowMS - timePrevMS) / 1000))

      })

    ssc.start()
    ssc.awaitTermination()

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
