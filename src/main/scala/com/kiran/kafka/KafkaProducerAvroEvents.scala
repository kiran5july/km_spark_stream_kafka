package com.kiran.kafka

/*
  Sends Avro record to Kafka topic: visioneventstopic
  Schema File: C:\km\km_spark_stream_kafka\src\main\scala\VisionEvent.avsc
  {"namespace": "vision.events",
 "type": "record",
 "name": "VisionAvroEvent",
 "fields": [
     {"name": "endpoint_id", "type": "long"},
     {"name": "duration",  "type": "int"},
     {"name": "domain_id", "type": "int"}
 ]
}
 */
import java.sql.Timestamp

import com.utils.VisionUtils
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Date, Properties, UUID}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
//import domain.User
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream
import org.apache.avro.io._
//import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.reflect.ClassTag
import scala.collection.JavaConversions._


object KafkaProducerAvroEvents{

  val logger = Logger.getLogger(this.getClass().getName())

  val bootstrapServers = "lxe0962.allstate.com:9092"
                         //"localhost:9092" //"lxe0730.allstate.com:9092,lxe0731.allstate.com:9092,lxe0731.allstate.com:9092"
  val kafkaTopic = //"rtalab.allstate.is.vision.ingest"
                    //"km.vision.events.topic" // Comma separated list of topics
                    //"rtalab.allstate.is.vision.test"
                    //"rtalab.allstate.is.vision.alerts_durga"
                    "rtalab.allstate.is.vision.test10"
                    //"rtalab.allstate.is.vision.test25"
  val groupId = "VisionEvents_Batch_Producer_001"
  val schemaRegistryUrl = "http://lxe0961.allstate.com:8081"

  val props = new java.util.Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("message.send.max.retries", "5")
  props.put("request.required.acks", "-1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //classOf[org.apache.kafka.common.serialization.StringDeserializer]
  //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put("client.id", UUID.randomUUID().toString())
  props.put("security.protocol", "SASL_PLAINTEXT")
  props.put("sasl.kerberos.service.name", "kafka")

  var valueSerializer: KafkaAvroSerializer = _

 def main(args: Array[String]): Unit = {

   logger.info(getDT() + ": " + this.getClass().getName() + ": Starting main...")

   //logger.log(Level.INFO, "main........")
   //val conf = new SparkConf()
   //  .setAppName("Spark Producer of Avro data to Kafka")
   //  .setMaster("local[1]")
   //val sc = new SparkContext(conf)
   //sc.setLogLevel("ERROR")

   //val schema: Schema = com.utils.VisionUtils.getVisionEventSchema()
   val schema: Schema = com.utils.VisionUtils.getRESTSchemaBySubject(schemaRegistryUrl, kafkaTopic+"-value")
   println(getDT() + ":Schema : "+schema)

   //random data generator
   import scala.util.Random
   //val endpointsList = List(157893, 17, 22283, 27, 103, 169, 36691)
   //val endpointsList = List("252495","252579", "254007", "278337") //for ingest
   var endpointsList = 1 to 9999999
   println(getDT() + ":Random endpoint_id="+ endpointsList(new Random().nextInt(endpointsList.size)) )

   val durationList = List(24110, 10000, 50000, 8000, 22000) //1,2,3) //3000 to 5000
   println(getDT() + ": Random duration="+ durationList(new Random().nextInt(durationList.size)) )

   val domain_idList = List(111, 112, 113, 114, 115, 116, 117, 118, 119, 120)
   println(getDT() + ": Random endpoint_id="+ domain_idList(new Random().nextInt(domain_idList.size)) )

   val bErrorList = List(true, false)
   println(getDT() + ": Random booleanList="+ bErrorList(new Random().nextInt(bErrorList.size)) )

   println(getDT() + ": Building producer..")
   val producer = new KafkaProducer[String, Array[Byte]](props)
   println(getDT() + ": Building producer.. done")

   /*
   //#1: Using avro generic record object
   val genEvent: GenericRecord = new GenericData.Record(schema)

   for (i <- 1 to 10) {
     genEvent.put("endpoint_id", endpoints(new Random().nextInt(endpoints.size)).toInt) //.toLong
     genEvent.put("duration", duration(new Random().nextInt(duration.size)).toInt) //.toLong
     genEvent.put("domain_id", domain_id(new Random().nextInt(domain_id.size)).toInt) //.toLong

     genEvent.put("application_id", 0L)
     genEvent.put("host_id", 0L)
     genEvent.put("status_code", 0L)
     genEvent.put("error_occurred", false)
     genEvent.put("span_created_at", System.currentTimeMillis())
     println("GenericRecord built.")

     // Serialize generic record into byte array
     val byteStrm = new ByteArrayOutputStream()
     val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(byteStrm, null)
     val writer = new SpecificDatumWriter[GenericRecord](schema) //VERIFY
     writer.write(genEvent, encoder) //VERIFY
     encoder.flush()
     byteStrm.close()

     println("Sending data...")
     println("Sending data= " + byteStrm.toByteArray())
     producer.send(new ProducerRecord[String, Array[Byte]](sTopic, genEvent.get("endpoint_id").toString(), byteStrm.toByteArray()))
     println("Sent at " + getDT())
   }
*/

   //#2: Using KafkaAvroSerializer
   val schemaRegProps = Map("schema.registry.url" -> schemaRegistryUrl)
   val client = new CachedSchemaRegistryClient(schemaRegistryUrl, Int.MaxValue)
   valueSerializer = new KafkaAvroSerializer(client)
   valueSerializer.configure(schemaRegProps, false)
   //val currentTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
   //val currentTime = new Timestamp(System.currentTimeMillis)
var duration = 0
   var avroRecord: GenericRecord = null
   println(getDT() + ": Starting the producer..")
   for (i <- 1 to 200) {
   //for(i <- 5689400 to 5689499) {

     val endpoint_id = endpointsList(new Random().nextInt(endpointsList.size))
     //val endpoint_id = 40991
     duration = durationList(new Random().nextInt(durationList.size)).toInt
     //duration = 0
     //duration = duration + 1
     val domain_id = domain_idList(new Random().nextInt(domain_idList.size))

     avroRecord = new GenericData.Record(schema)
     avroRecord.put("endpoint_id", endpoint_id)
     avroRecord.put("duration", duration)
     avroRecord.put("domain_id", domain_id) //Int
     avroRecord.put("application_id", 0L) //Long
     avroRecord.put("host_id", 0) //Int; earlier it was 0L
     avroRecord.put("method", "GET")
     avroRecord.put("status_code", 0) //Int; earlier it was 0L
     avroRecord.put("error_occurred", bErrorList(new Random().nextInt(bErrorList.size)))
     avroRecord.put("span_created_at", System.currentTimeMillis())

     //println(  getDT()+":"+ i.toString() +":"+System.currentTimeMillis() +": sending to "+kafkaTopic+"; key=null ; endpoint_id="+endpoint_id+", duration="+duration+", domain_id="+domain_id+")")
     println( getDT()+":"+ i.toString() +":"+System.currentTimeMillis() +": sending to "+kafkaTopic+"; " + avroRecord.toString())
     //producer.send(new ProducerRecord[String, Array[Byte]](kafkaTopic, null , valueSerializer.serialize(kafkaTopic, avroRecord).toArray))
     producer.send(new ProducerRecord[String, Array[Byte]](kafkaTopic, endpoint_id.toString(), valueSerializer.serialize(kafkaTopic, avroRecord).toArray))
     //(endpoint_id, statRecord)
     println( getDT()+":"+ i.toString() +":sent.")
     producer.flush()
     //if(i%100 == 0){
       println(getDT()+": --> Sleeping now..")
       Thread.sleep(10) //15000
       println(getDT()+": --> Woke up")
     //}

   }

   producer.close()

 }

  def getDT(): String = {

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }

  def serialise(value: Any): Array[Byte] = {
    import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  def deserialise(bytes: Array[Byte]): Any = {
    import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value
  }

  def getType[T](v: T) = v
}
