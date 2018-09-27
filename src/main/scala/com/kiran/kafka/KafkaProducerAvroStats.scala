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

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Date, UUID}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.reflect.ClassTag
//import domain.User
import java.io.ByteArrayOutputStream

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

object KafkaProducerAvroStats{

  val logger = Logger.getLogger(this.getClass().getName())

  val bootstrapServers = "lxe0962.allstate.com:9092"
                          //"localhost:9092" //"lxe0730.allstate.com:9092,lxe0731.allstate.com:9092,lxe0731.allstate.com:9092"
  val kafkaTopic = "rtalab.allstate.is.vision.stats"
                    //"km.vision.endpointstats.topic.test2" // Comma separated list of topics
                    //"rtalab.allstate.is.vision.alerts_kiranXXXX"
  val groupId = "VisionEventsConsumer_001"
  //val streamingWindow = 10 // in seconds
  //val configRefreshWindow = 300 // 30 minutes, expressed in seconds
  val schemaRegistryUrl = "http://lxe0961.allstate.com:8081"

  val props = new java.util.Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("message.send.max.retries", "5")
  props.put("request.required.acks", "-1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put("security.protocol", "SASL_PLAINTEXT")
  props.put("sasl.kerberos.service.name", "kafka")
  //props.put("client.id", UUID.randomUUID().toString())

  var valueSerializer: KafkaAvroSerializer = _

  implicit val formats = Serialization.formats(NoTypeHints)

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)


 def main(args: Array[String]): Unit = {

   //logger.log(Level.INFO, "main........")
   val conf = new SparkConf().setAppName("Spark Producer of Event Stats to Kafka").setMaster("local[10]")
   val sc = new SparkContext(conf)
   sc.setLogLevel("INFO")

   //val sTopic = kafkaTopic.split(",").last //rtalab.allstate.is.vision.mathew
   //logger.info("main : kafkaParamsProp = " + props + "   topics= " + sTopic)

   //Get avro schema
   //val schema: Schema = com.utils.VisionUtils.getVisionEventStatsSchema()
   val schema: Schema = com.utils.VisionUtils.getRESTSchemaBySubject(schemaRegistryUrl, kafkaTopic+"-value")
   println("Schema : "+schema)

   //random data generator
   import scala.util.Random

   //val endpointsList = List("1000001", "1000002", "1000003", "1000004", "1000005", "1000006", "1000007", "1000008", "1000009", "1000010")
   //val endpointsList = List("5689400", "5689403","5689407", "5689411", "5689413", "5689433", "5689447", "5689465", "5689477", "5689499")
   val endpointsList = List(-1)
   println("Random endpoint_id="+ endpointsList(new Random().nextInt(endpointsList.size)) )

   val meanList = List("0.7", "0.9", "1.1", "1.4", "1.5", "1.8")
   print("Random duration="+ meanList(new Random().nextInt(meanList.size)) )

   val stdevList = List("0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8")
   print("Random endpoint_id="+ stdevList(new Random().nextInt(stdevList.size)) )

   println("Building producer..")
   val producer = new KafkaProducer[String, Array[Byte]](props)  //Long
   println("Building producer.. done")
/*
   //#1: Create avro generic record object
   val genEvent: GenericRecord = new GenericData.Record(schema)

   for (i <- 1000000 to 1000010) {
     genEvent.put("endpoint_id", i.toLong)
     genEvent.put("mean", meanList(new Random().nextInt(meanList.size)).toDouble)
     genEvent.put("stddev", stdevList(new Random().nextInt(stdevList.size)).toDouble)

     genEvent.put("createTimestamp", System.currentTimeMillis())
     println("GenericRecord built.")

     // Serialize generic record into byte array
     val byteStrm = new ByteArrayOutputStream()
     val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(byteStrm, null)
     val writer = new SpecificDatumWriter[GenericRecord](schema) //VERIFY
     writer.write(genEvent, encoder) //VERIFY
     encoder.flush()
     byteStrm.close()

     println("Sending data...")
     //println("Sending data= " + byteStrm.toByteArray())
     producer.send(new ProducerRecord[String, Array[Byte]](sTopic, genEvent.get("endpoint_id").toString(), byteStrm.toByteArray()))
     //producer.send(new ProducerRecord[Long, Array[Byte]](sTopic, genEvent.get("endPoint_id").toString().toLong(), byteStrm.toByteArray()))
     println("Sent at " + getDT())
   }
*/

   //#2: Using KafkaAvroSerializer
   val kafkaProps = Map("schema.registry.url" -> schemaRegistryUrl)
   val client = new CachedSchemaRegistryClient(schemaRegistryUrl, 20)
   valueSerializer = new KafkaAvroSerializer(client)
   valueSerializer.configure(kafkaProps, false)
   //val currentTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))

   var avroRecord: GenericRecord = null
   for (i <- 1 to 2) {
   //for( i <- 9999999 to 9999999) {
     //val endpoint_id = i
     val endpoint_id = endpointsList(new Random().nextInt(endpointsList.size)) //.toInt
     val mean = meanList(new Random().nextInt(meanList.size)).toDouble
     val stddev = stdevList(new Random().nextInt(stdevList.size)).toDouble

     val err_mean = meanList(new Random().nextInt(meanList.size)).toDouble
     val err_stddev = stdevList(new Random().nextInt(stdevList.size)).toDouble

     avroRecord = new GenericData.Record(schema)
     avroRecord.put("endpoint_id", endpoint_id)

     //avroRecord.put("mean", mean)
     //avroRecord.put("stddev", stddev)
     val statsDur = new GenericData.Record(schema.getField("duration").schema()) //KM: check
     statsDur.put("mean", mean)
     statsDur.put("stddev", stddev)
     avroRecord.put("duration", statsDur)

     //avroRecord.put("error_stats.err_mean", err_mean)
     //avroRecord.put("error_stats.err_stddev", err_stddev)
     val statsErr = new GenericData.Record(schema.getField("errors").schema()) //KM: check
     statsErr.put("mean", mean)
     statsErr.put("stddev", stddev)
     avroRecord.put("errors", statsErr)

     avroRecord.put("create_timestamp", System.currentTimeMillis)

     val statRecord = valueSerializer.serialize(kafkaTopic, avroRecord)
     println(getDT()+": sending key="+endpoint_id+"; rec: ("+endpoint_id+","+mean+","+stddev+"); Var Type="+ getType(statRecord)+"; " + statRecord.toArray)
     producer.send(new ProducerRecord[String, Array[Byte]](kafkaTopic, endpoint_id.toString(), statRecord.toArray))
     println(getDT()+": sent.")
     //(endpoint_id, statRecord)
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
