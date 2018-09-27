package com.kiran.kafka

/***************
*Reads messages from Kafka topic

 */
//import com.utils.VisionUtils.{VisionEvent}
import java.io.{PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Date

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.avro.generic.{GenericData, GenericRecord}
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


object SparkStructStreamKafkaAvroMessages {
  val logger = Logger.getLogger(this.getClass().getName())

  var statsSchema: Schema = _
  var valueDeserializer: KafkaAvroDeserializer = _

  def main(args: Array[String]) {

    logger.info("Class = " + this.getClass().getName())
    logger.info(getDT() + ": SparkStreamKafkaTopic start.")

    val bootstrapServers = "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092"
    //"localhost:9092" //"lxe0730.allstate.com:9092,lxe0731.allstate.com:9092,lxe0731.allstate.com:9092"

    val kafkaTopicName = //"rtalab.allstate.is.vision.ingest"
    //  "rtalab.allstate.is.vision.stats"
    //"rtalab.allstate.is.vision.alerts"
    "rtalab.allstate.is.vision.test10" //events
    //"rtalab.allstate.is.vision.alerts_kiran" //stats
    //"rtalab.allstate.is.vision.alerts_durga" //alerts

    val grpId_strm = "KM_TOPIC_Messages_0003"
    val schemaRegistryUrl = "http://lxe0961.allstate.com:8081"

    //Using SparkSession & SparkContext
    /*  // *IMP* Replaced this because of lz4 NoMethodFound error
        val spark = SparkSession
          .builder()
          .master("local[1]")
          .appName("Spark Streaming data from Kafka")
          .getOrCreate()

        spark.conf.set("spark.io.compression.codec", "snappy")
    */
    val conf = new SparkConf()
      .setAppName("Spark Struct Streaming data from Kafka")
      .setMaster("local[4]")
      .set("spark.io.compression.codec", "snappy") //I think default using lz4

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate

    //spark.conf.getAll.mkString("\n").foreach(print)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    try {
      import spark.sqlContext.implicits._
      val topicData = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("subscribe", kafkaTopicName)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load
        .mapPartitions(currentPartition => {
          println("*** mapPartitions: Getting Schema ***")
          val valueSchema = new RestService(schemaRegistryUrl).getLatestVersion(kafkaTopicName + "-value")
          println("*** Schema: " + valueSchema.getSchema)
          statsSchema = new Schema.Parser().parse(valueSchema.getSchema)
          println("*** schema parsed: " + statsSchema)
          //statsSchema = parser.parse("{\"type\":\"record\",\"name\":\"visionStats\",\"fields\":[{\"name\":\"endpoint_id\",\"type\":\"long\"},{\"name\":\"mean\",\"type\":\"double\",\"default\":0.0},{\"name\":\"stddev\",\"type\":\"double\",\"default\":0.0},{\"name\":\"createTimestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},\"default\":0}]}")

          println("*** valueDeserializer ***")
          valueDeserializer = new KafkaAvroDeserializer( new CachedSchemaRegistryClient(schemaRegistryUrl, 20) )
          valueDeserializer.configure(Map("schema.registry.url" -> schemaRegistryUrl), false)

          //println("*** Map.. ***")
          currentPartition.map(rec => {

            //val topic = rec.getAs[String]("topic")
            val offset = rec.getAs[Long]("offset")
            val partition = rec.getAs[Int]("partition")

            var event: GenericRecord = null
            try{
              event =  valueDeserializer.deserialize(kafkaTopicName, rec.getAs[Array[Byte]]("value"), statsSchema).asInstanceOf[GenericRecord]
              (event.get("endpoint_id").asInstanceOf[Integer],
                event.get("duration").asInstanceOf[Integer],
                event.get("error_occurred").asInstanceOf[Boolean],
                event.get("span_created_at").asInstanceOf[Long])
              //For Not to deserialize
              //event = rec.getAs[Array[Byte]]("value").toString()
            } catch {
              case e: Exception =>
                logger.error("Error occurred while deserializing the message at partition: " + partition + ", offset: " + offset)
                logger.error(e.getMessage)
                (new Integer(-1), new Integer(0), false, 0L) //Events
            }

          })
        }).toDF("endpoint_id", "duration", "error_occurred", "span_created_at")

      //println("*** DF cache & Rec count= "+statsData.count()) //1813
      //statsData.show(2000, false)

      if(topicData.head(1).length <= 0) {
        logger.error("*** Alert! Missing stats/reference data. ***")
        println("*** Alert! Missing stats/reference data. ***")
      }else {

        topicData.show(50, false)

        import org.apache.spark.sql.functions._
        val epCount = topicData
            .filter($"endpoint_id" >= 9999 )
          .groupBy($"endpoint_id")
          .agg(count($"endpoint_id")
          )
        println("*** endpoint_id count= " + epCount.count() + " ***")
        epCount.show(20, false)

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
