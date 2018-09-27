package com.kiran.kafka

/*
  Sends Avro record to Kafka topic: visioneventstopic
  Schema File: C:\km\km_spark_stream_kafka\src\main\scala\VisionEventStats.avsc
{"namespace": "vision.events",
 "type": "record",
 "name": "VisionAvroEventStats",
 "fields": [
     {"name": "endpoint_id", "type": "long"},
     {"name": "mean",  "type": "float"},
     {"name": "stdev", "type": "float"}
 ]
}
 */

import java.text.SimpleDateFormat
import java.util.Date

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.reflect.ClassTag
//import domain.User
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

object KafkaProducerString {

  val logger = Logger.getLogger(this.getClass().getName())

  val bootstrapServers = "lxe0962.allstate.com:9092"
                          //"localhost:9092" //"lxe0730.allstate.com:9092,lxe0731.allstate.com:9092,lxe0731.allstate.com:9092"
  val kafkaTopic = "rtalab.allstate.is.vision.test"
                    //"km.vision.endpointstats.topic.test2" // Comma separated list of topics
                    //"rtalab.allstate.is.vision.alerts_durga"
  val groupId = "Kafka_producer_001"
  //val streamingWindow = 10 // in seconds
  //val configRefreshWindow = 300 // 30 minutes, expressed in seconds
  //val schemaRegistryUrl = "http://lxe0961.allstate.com:8081"

  val props = new java.util.Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("message.send.max.retries", "5")
  props.put("request.required.acks", "-1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("security.protocol", "SASL_PLAINTEXT")
  props.put("sasl.kerberos.service.name", "kafka")
  //props.put("client.id", UUID.randomUUID().toString())

  var valueSerializer: KafkaAvroSerializer = _

  implicit val formats = Serialization.formats(NoTypeHints)

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)


 def main(args: Array[String]): Unit = {

   //logger.log(Level.INFO, "main........")
   //val conf = new SparkConf().setAppName("Spark Producer of Event Stats to Kafka").setMaster("local[10]")
   //val sc = new SparkContext(conf)
   //sc.setLogLevel("INFO")

   //val sTopic = kafkaTopic.split(",").last //rtalab.allstate.is.vision.mathew
   //logger.info("main : kafkaParamsProp = " + props + "   topics= " + sTopic)

   //random data generator
   import scala.util.Random

   //val endpointsList = List("1000001", "1000002", "1000003", "1000004", "1000005", "1000006", "1000007", "1000008", "1000009", "1000010")
   //val endpointsList = List("5689400", "5689403","5689407", "5689411", "5689413", "5689433", "5689447", "5689465", "5689477", "5689499")
   //val keyList = List(111, 222, 333)
   val keyList = List("111", "222", "333")
   println("Random key="+ keyList(new Random().nextInt(keyList.size)) )

   val valsList = List("0.7", "0.9", "1.1", "1.4", "1.5", "1.8")
   print("Random val="+ valsList(new Random().nextInt(valsList.size)) )

   println("Building producer..")
   val producer = new KafkaProducer[String, String](props)  //Long
   println("Building producer.. done")


   var avroRecord: GenericRecord = null
   for (i <- 1 to 2) {
   //for( i <- 9999999 to 9999999) {
     //val key_id = i
     val key_id = keyList(new Random().nextInt(keyList.size))
     val valValue = valsList(new Random().nextInt(valsList.size)).toDouble

     println(getDT()+": sending key="+key_id+"; rec: ("+ valValue +"); ")
     producer.send(new ProducerRecord[String, String](kafkaTopic, key_id.toString(), valValue.toString()))
     println(getDT()+": sent.")
     producer.flush()
   }

   producer.close()

 }

  def getDT(): String = {

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }
  def getType[T](v: T) = v
}
