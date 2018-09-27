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
import org.apache.spark.sql.types.{FloatType, TimestampType, LongType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}


object SparkStreamKafkaConsumerKafkaAvroDeserInStr {
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

    val kafkaTopic = //"rtalab.allstate.is.vision.ingest" //DATA (key, val)
                    //"rtalab.allstate.is.vision.stats"
                    //"rtalab.allstate.is.vision.alerts"
                    //"rtalab.allstate.is.vision.test10" //Events
                  "rtalab.allstate.is.vision.alerts_kiran" //Stats
                  //  "rtalab.allstate.is.vision.alerts_durga" //Alerts
                    //"rtalab.allstate.is.vision.test" //DATA (null key, val)
                    //"rtalab.allstate.is.vision.results_spark" //NO DATA
                    //"rtalab.allstate.is.vision.results_str_spark" //NO DATA
                          //"km.vision.events.topic" // Comma separated list of topics

    val grpId_strm = "Kafka_Stream_KM_READER_01xx"
    val streamInterSec = 5 // in seconds
    //val refRefreshInterMin = 1 // 30 minutes
    val schemaRegistryUrl = "http://lxe0961.allstate.com:8081"


    val kafkaParamsEvt : Map[String, Object] = Map[String, Object](
      "group.id" -> System.currentTimeMillis().toString(), //grpId_strm
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      //"value.deserializer" -> classOf[StringDeserializer],
      //"value.deserializer" -> classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer], //manually deserialize
      "value.deserializer" -> classOf[KafkaAvroDeserializer], //automatically deserializes & returns json string
      "auto.offset.reset" -> "latest", //latest, earliest, none
      "schema.registry.url" -> schemaRegistryUrl,
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
    import spark.sqlContext.implicits._
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(streamInterSec))
    //StreamingContext without SparkContext
    //val ssc = new StreamingContext("local[2]", "Spark Streaming data from Kafka", Seconds(30))

    //Get schemas
    //From method - WORKS
    var sSchema: Schema = getRESTSchemaBySubject(schemaRegistryUrl, kafkaTopic+"-value")
    println(getDT() + ": schema= " + sSchema)

    val restService = new RestService(schemaRegistryUrl)
    val valueSchema = restService.getLatestVersion(kafkaTopic + "-value")
    sSchema = (new Schema.Parser).parse(valueSchema.getSchema)
    println(getDT() + ": schema= " + sSchema)


    println(getDT() + ": Building stream..")
    val stream =
      KafkaUtils.createDirectStream[String, Array[Byte]](
        //KafkaUtils.createDirectStream[String, String](
        //KafkaUtils.createDirectStream[String, KafkaAvroDeserializer](
        ssc,
        PreferConsistent,
        Subscribe[String, Array[Byte]](kafkaTopic.split(","), kafkaParamsEvt)
        //Subscribe[String, String](kafkaTopic.split(","), kafkaParamsEvt)
      ) //.map(_.value.toString)
      .map(x => {
        try {

          val rec = x.value.asInstanceOf[GenericRecord]
          //println(getDT()+": message: "+ rec.toString() )

//event
          (rec.get("endpoint_id").asInstanceOf[Integer], rec.get("duration").asInstanceOf[Integer],
            rec.get("error_occurred").asInstanceOf[Boolean], rec.get("span_created_at").asInstanceOf[Long])

//stats
          /* OLD (rec.get("endpoint_id").asInstanceOf[Integer],   rec.get("mean").asInstanceOf[Double], rec.get("stddev").asInstanceOf[Double],
            rec.get("err_mean").asInstanceOf[Double], rec.get("err_stddev").asInstanceOf[Double],
            rec.get("create_timestamp").asInstanceOf[Long])
*/
          //Stats:NEW
/*          val stats_duration = rec.get("duration").asInstanceOf[GenericRecord]
          val stats_errors = rec.get("errors").asInstanceOf[GenericRecord]
          if(stats_duration != null){
          (rec.get("endpoint_id").asInstanceOf[Integer], //Integer
            stats_duration.get("mean").asInstanceOf[Double],
            stats_duration.get("stddev").asInstanceOf[Double],

            stats_errors.get("mean").asInstanceOf[Double],
            stats_errors.get("stddev").asInstanceOf[Double],
            rec.get("create_timestamp").asInstanceOf[Long])
          }
          else
            (new Integer(-1), 0.0, 0.0, 0.0, 0.0, 0L )
*/
//Alerts
          /*OLD(rec.get("endpoint_id").asInstanceOf[Integer]
            , rec.get("zscore0To1").asInstanceOf[Integer], rec.get("zscore1To2").asInstanceOf[Integer],
            rec.get("zscore2To3").asInstanceOf[Integer], rec.get("zscoreAbove3").asInstanceOf[Integer],
            //rec.get("errors").asInstanceOf[Integer],
            rec.get("timestamp").asInstanceOf[Long],
            rec.get("errzscore0To1").asInstanceOf[Integer], rec.get("errzscore1To2").asInstanceOf[Integer],
            rec.get("errzscore2To3").asInstanceOf[Integer], rec.get("errzscoreAbove3").asInstanceOf[Integer]
            )*/
        //Alerts:NEW
          val stats_duration = rec.get("duration").asInstanceOf[GenericRecord]
          val stats_errors = rec.get("errors").asInstanceOf[GenericRecord]
          //if(stats_duration != null){
            (rec.get("endpoint_id").asInstanceOf[Integer], //Integer
                stats_duration.get("mean").asInstanceOf[Double],
              stats_duration.get("zscoreUnder_3").asInstanceOf[Int],
              stats_duration.get("zscore_2To_3").asInstanceOf[Int],
              stats_duration.get("zscore_1To_2").asInstanceOf[Int],
              stats_duration.get("zscore1To_1").asInstanceOf[Int],
              stats_duration.get("zscore1To2").asInstanceOf[Int],
              stats_duration.get("zscore2To3").asInstanceOf[Int],
              stats_duration.get("zscoreAbove3").asInstanceOf[Int],

              stats_errors.get("mean").asInstanceOf[Double],
              stats_errors.get("zscoreUnder_3").asInstanceOf[Int],
              stats_errors.get("zscore_2To_3").asInstanceOf[Int],
              stats_errors.get("zscore_1To_2").asInstanceOf[Int],
              stats_errors.get("zscore1To_1").asInstanceOf[Int],
              stats_errors.get("zscore1To2").asInstanceOf[Int],
              stats_errors.get("zscore2To3").asInstanceOf[Int],
              stats_errors.get("zscoreAbove3").asInstanceOf[Int],
                rec.get("timestamp").asInstanceOf[Long])
          //}
          //else
            //(0, 0.0, 1,1,1,1,1,1,1,1, 0.0, 1,1,1,1,1, 0L )


          } catch {
          case e: Exception =>
            logger.info("Message is not validated against the schema... " + e.getMessage)
            println("Message is not validated against the schema... " + e.getMessage)
            //(new Integer(-1), new Integer(0), false, 0L) //Events
            //(new Integer(-1), 0.0, 0.0, 0.0, 0.0, 0L) //Stats
            (new Integer(-1), 0.0, new Integer(0), new Integer(0), new Integer(0), new Integer(0), new Integer(0), new Integer(0), new Integer(0)
                            , 0.0, new Integer(0), new Integer(0), new Integer(0),new Integer(0), new Integer(0), new Integer(0), new Integer(0), 0L)
          }
        })

      timeNowMS = System.currentTimeMillis()

      //process each RDD
      println(getDT() + ": Processing stream..")
      stream.foreachRDD( (rdd) => {
      //(rdd, time: Time) => {
      //println(getDT() + ": foreachRDD...")

      if (!rdd.isEmpty()) {
      //val commitOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          println(getDT() + ": rdd is NOT empty; rows= " + rdd.count())  //rows= 27864
          //rdd.foreach(println)
          try {

            //rdd.foreachPartition(itr => {
            import org.apache.spark.sql.functions._
            import spark.sqlContext.implicits._

            //Option#1: WORKS; print as this
            val dfEvents = rdd
                //.filter(x => x > 0)
                //.foreach(println)

            //Events
/*                .map(x => (x._1, x._2, x._3, x._4))
                .toDF("endpoint_id", "duration", "error_occurred", "span_created_at")
                .withColumn("timestamp_DT", (col("span_created_at").cast(FloatType)/1000).cast(TimestampType))
*/
            //Stats
            /*    .map(x => (x._1, x._2, x._3, x._4, x._5, x._6) )
                .toDF("endpoint_id", "dur_mean", "dur_stddev", "err_mean", "err_stddev", "create_timestamp")
                .withColumn("timestamp_DT", (col("create_timestamp").cast(LongType)/1000).cast(TimestampType))
*/
            //Alerts
                .map(x => (x._1, x._2, x._3.asInstanceOf[Int], x._4.asInstanceOf[Int], x._5.asInstanceOf[Int], x._6.asInstanceOf[Int]
                      ,x._7.asInstanceOf[Int], x._8.asInstanceOf[Int], x._9.asInstanceOf[Int]
                      , x._10, x._11.asInstanceOf[Int], x._12.asInstanceOf[Int], x._13.asInstanceOf[Int], x._14.asInstanceOf[Int]
                      , x._15.asInstanceOf[Int], x._16.asInstanceOf[Int], x._17.asInstanceOf[Int], x._18))
                .toDF("endpoint_id", "dur_mean", "dur_zscoreUnder_3", "dur_zscore_2To_3", "dur_zscore_1To_2", "dur_zscore1To_1", "dur_zscore1To2", "dur_zscore2To3", "dur_zscoreAbove3"//, "errors"
                      ,"err_mean", "err_zscoreUnder_3", "err_zscore_2To_3", "err_zscore_1To_2", "err_zscore1To_1","err_zscore1To2","err_zscore2To3","err_zscoreAbove3", "timestamp")
                .withColumn("timestamp_DT", (col("timestamp").cast(FloatType)/1000).cast(TimestampType))
                ////.filter($"err_mean" > 0.0)
                ////.filter($"endpoint_id" === 17)
                ////.filter($"timestamp_DT" >= "2018-05-31 00:00:00") // && $"timestamp_DT" >= "2018-05-30 20:00:00")

                //println("---> Records count="+ dfEvents.count())
            //dfEvents.printSchema()

            dfEvents.
               //filter($"key" =!= null)
               //filter($"key" === "252495")
                //filter($"errors" > 0)
                sort($"endpoint_id").
               show(100, false)


          }catch{
          case e: Exception => println("*** Exception.. Continuing to next RDD...")
              e.printStackTrace()
          }


        } else {
          logger.info("It seems there are no events flowing in. -KM- Received an empty RDD!")
          println("It seems there are no events flowing in. -KM- Received an empty RDD!")
        }

        //println(getDT() + ": Completed.")
        timePrevMS = timeNowMS
        timeNowMS = System.currentTimeMillis()
        println(getDT() + ": Kafka poll Started@ "+timePrevMS+ "/ " + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(timePrevMS)) +
          ")  -->  ("+timeNowMS+" / " + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(timeNowMS)) +
          ") = " + ((timeNowMS - timePrevMS) / 1000))

      })

      ssc.start()
      ssc.awaitTermination()

}

  @throws(classOf[Exception])
  def getRESTSchemaBySubject(schemaURL: String, subjectName: String) : org.apache.avro.Schema = {

    Try(new RestService(schemaURL).getLatestVersion(subjectName).getSchema) match {
      case Success(s) => {
        logger.info(s"Found schema for $subjectName")
        println(s"Found schema for $subjectName")
        new Schema.Parser().parse(s)
      }
      case Failure(f) => {
        logger.warn("Unable to connect to the Schema registry. An attempt will be made to create the table" +
        " on receipt of the first records.")
        null
      }
    }

  }
  //Manual deserialization
  //def decodeAvroBytes(schema: org.apache.avro.Schema)(cr: ConsumerRecord[String, Array[Byte]]): (String, String) = {
  def decodeEventAvroBytes(schema: org.apache.avro.Schema, cr: ConsumerRecord[String, Array[Byte]])
  : (String, String) = {
      try {
        println(getDT() + "; decodeEventAvroBytes : Entry" )
        val visionGenRec: GenericRecord = new GenericData.Record(schema)
        val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
        //val reader: DatumReader[VisionAvroEvent] = new SpecificDatumReader[VisionAvroEvent](schema)
        val decoder: Decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(cr.value()), null)
        val eventRec: GenericRecord = reader.read(visionGenRec, decoder)
        println(getDT() + "; decodeEventAvroBytes : got Aro - String." )
        //(visionGenRec.get("endPoint_id").toString, (visionGenRec.get("duration").toString, visionGenRec.get("domain_id").toString))
        (eventRec.get("endpoint_id").toString(), eventRec.get("domain_id").toString() + "," + eventRec.get("duration").toString())
      }catch{
      case e: Exception => e.printStackTrace
      ("", "")
      }
  }

  //Using KafkaAvroDeserializer
  //ConsumerRecord(topic = rtalab.allstate.is.vision.stats, partition = 0, offset = 14098, CreateTime = 1524853284239, checksum = 1370490035, serialized key size = 7, serialized value size = 30, key = 1000001, value = {"endpoint_id": 1000001, "mean": 0.9, "stddev": 0.2, "createTimestamp": 1524853283831}
  def decodeAvroDeser(cr: ConsumerRecord[String, Array[Byte]])
  : (String, String) = {
    try {

      println(getDT() + "; decodeEventAvroBytes : Entry" )
      var avroRec = cr.value().asInstanceOf[GenericRecord]
      (avroRec.get("endpoint_id").toString(), avroRec.get("domain_id").toString() + "," + avroRec.get("duration").toString())

      //val rec = (avroRec.get("endpoint_id"), avroRec.get("mean"), avroRec.get("stddev"))
      //println (getDT () + ":foreach Record: Key=" + cr.key () + "; Value= " + rec._1+","+rec._2+","+rec._3 +"; " + getType(cr))
      //(rec._1.toString(), rec._2+","+rec._3 )
    }catch{
    case e: Exception => e.printStackTrace
    ("", "")
    }
  }

  def decodeStatsAvroBytes(schema: org.apache.avro.Schema, cr: ConsumerRecord[String, Array[Byte]])
  : (String, String) = {

    try{
      println(getDT() + "; decodeStatsAvroBytes with schema: "+schema)
      val visionGenRec: GenericRecord = new GenericData.Record(schema)
      val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
      //val reader: DatumReader[VisionAvroEvent] = new SpecificDatumReader[VisionAvroEvent](schema)
      val decoder: Decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(cr.value()), null)
      val eventRec: GenericRecord = reader.read(visionGenRec, decoder)

      //(eventRec.get("endPoint_id").toString(), (eventRec.get("mean").toString(), eventRec.get("stdev").toString()))
      (eventRec.get("endpoint_id").toString(), eventRec.get("mean").toString()+","+ eventRec.get("stdev").toString())
    }catch{
    case e: Exception => e.printStackTrace
     ("", "")
    }
  }

  def decodeAlertsAvroBytes(schema: org.apache.avro.Schema, cr: ConsumerRecord[String, Array[Byte]])
  : (String, String) = {

  try{
  println(getDT() + "; decodeAlertsAvroBytes with schema: "+schema)
  val visionGenRec: GenericRecord = new GenericData.Record(schema)
  val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
  //val reader: DatumReader[VisionAvroEvent] = new SpecificDatumReader[VisionAvroEvent](schema)
  val decoder: Decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(cr.value()), null)
  val eventRec: GenericRecord = reader.read(visionGenRec, decoder)

  //(eventRec.get("endPoint_id").toString(), (eventRec.get("mean").toString(), eventRec.get("stdev").toString()))
  (eventRec.get("endpoint_id").toString(),
  "STATS:" + eventRec.get("zscore0To1").toString()
  +","+ eventRec.get("zscore1To2").toString()
  +","+eventRec.get("zscore2To3").toString()
  +","+eventRec.get("zscoreAbove3").toString()
  +","+eventRec.get("errors").toString()
  +","+eventRec.get("timestamp").toString())
  }catch{
  case e: Exception => e.printStackTrace
  ("", "")
  }
  }

  //For DF
  def encodeToAvroBytesDF(schema: org.apache.avro.Schema)(row: Row)
  : Array[Byte] = {
  val gr: GenericRecord = new GenericData.Record(schema)
  row.schema.fieldNames.foreach(name => gr.put(name, row.getAs(name)))

  val writer = new SpecificDatumWriter[GenericRecord](schema)
  val out = new ByteArrayOutputStream()
  val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
  writer.write(gr, encoder)
  encoder.flush()
  out.close()

  out.toByteArray()
  }

  def encodeToAvroBytes(genEvent: GenericRecord,
                schema: org.apache.avro.Schema,
                propsKafkaResults: java.util.Properties,
                producer: KafkaProducer[String, Array[Byte]], kafkaTopicResp: String)
               //(row: Row)//: (String, Array[Byte])
               (row: (Long, Long, Long, Long, Long, Long))
  {
    try{
      println(getDT() + ": encodeToAvroBytes - Class=" + row.getClass()+"; Row=" + row.toString())

      genEvent.put("endPoint_id", row._1.toLong)
      genEvent.put("z0_1", row._2.toLong)
      genEvent.put("z1_2", row._3.toLong)
      genEvent.put("z2_3", row._4.toLong)
      genEvent.put("z3", row._5.toLong)
      genEvent.put("evt_count", row._6.toLong)
      //println("GenericRecord built.")

      // Serialize generic record into byte array
      val byteStrm = new ByteArrayOutputStream()
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(byteStrm, null)
      val writer = new SpecificDatumWriter[GenericRecord](schema)
      writer.write(genEvent, encoder) //VERIFY
      encoder.flush()
      byteStrm.close()

      //println("Sending data= " + byteStrm.toByteArray())
      //producer.send(new ProducerRecord[String, Array[Byte]](kafkaTopicResp, genEvent.get("endPoint_id").toString(), byteStrm.toByteArray()))
      println("Sent at " + getDT())
      //(genEvent.get("endPoint_id").toString(), byteStrm.toByteArray())
    }catch{
    case e: Exception => e.printStackTrace
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
