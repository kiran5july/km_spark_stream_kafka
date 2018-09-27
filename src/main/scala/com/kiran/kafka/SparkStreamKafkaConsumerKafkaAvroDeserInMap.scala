package com.kiran.kafka

//import com.utils.VisionUtils.{VisionEvent}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema

import scala.util.{Failure, Success, Try}
import java.util.{Date, UUID}

import org.apache.avro.io.BinaryEncoder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DatumReader
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{Decoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import org.apache.spark.sql.types.{DateType, FloatType, LongType, TimestampType}


object SparkStreamKafkaConsumerKafkaAvroDeserInMap {
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
    "rtalab.allstate.is.vision.alerts"
    //"rtalab.allstate.is.vision.test10"
    //"rtalab.allstate.is.vision.alerts_kiran"
    //"rtalab.allstate.is.vision.alerts_durga"
    //"rtalab.allstate.is.vision.results_spark"
    //"rtalab.allstate.is.vision.results_str_spark"
    //"km.vision.events.topic" // Comma separated list of topics

    val grpId_strm = "Kafka_Stream_KM_0001"
    val streamInterSec = 10 // in seconds
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
      .setAppName("Consume Vision span events_KM")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
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
    val parser = new Schema.Parser
    sSchema = parser.parse(valueSchema.getSchema)
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


    timeNowMS = System.currentTimeMillis()

    import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    //process each RDD
    stream.foreachRDD( (rdd) => {
      //(rdd, time: Time) => {
      //println(getDT() + ": foreachRDD...")

      if (!rdd.isEmpty()) {
      //val commitOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //println(getDT() + ": rdd is NOT empty; rows= " + rdd.count())  //rows= 27864
      try {

        //rdd.foreachPartition(itr => {

        //Convert to DF & apply filter
//Events
        /*val df = rdd.map(x => {
          val rec = x.value.asInstanceOf[GenericRecord]
          (rec.get("endpoint_id").asInstanceOf[Integer], //Integer
            rec.get("duration").asInstanceOf[Integer], //Integer
            rec.get("error_occurred").asInstanceOf[Boolean],
            rec.get("span_created_at").asInstanceOf[Long])
        })
          .toDF("endpoint_id", "duration", "error_occurred", "span_created_at")
          .sort($"endpoint_id", $"span_created_at".desc)
*/
//stats-NEW
        /*val df = rdd.map(x => {
          val rec = x.value.asInstanceOf[GenericRecord]
          println(getDT() + ":rec=" + rec.toString() )
          //println(getDT() + ":duration=" + rec.get("duration").asInstanceOf[GenericRecord])
          var stats_duration = rec.get("duration").asInstanceOf[GenericRecord]

          //println(getDT() + ":errors=" + rec.get("errors").toString())
          var stats_errors = rec.get("errors").asInstanceOf[GenericRecord]
        if(stats_duration != null){
          //println(getDT() + ":endpint_id="+rec.get("endpoint_id").asInstanceOf[Integer])
          //println(getDT() + ":duration.mean="+stats_duration.get("mean").asInstanceOf[Double])
          //println(getDT() + ":duration.stddev="+stats_duration.get("stddev"))
          //println(getDT() + ":errors.mean="+stats_errors.get("mean"))
          //println(getDT() + ":errors.stddev="+stats_errors.get("stddev"))

          (rec.get("endpoint_id").asInstanceOf[Integer], //Integer
            stats_duration.get("mean").asInstanceOf[Double],
            stats_duration.get("stddev").asInstanceOf[Double],
            stats_errors.get("mean").asInstanceOf[Double],
            stats_errors.get("stddev").asInstanceOf[Double],
            rec.get("create_timestamp").asInstanceOf[Long])
          //(0, 0.0, 0.0, 0.0, 0.0, 0L)
        }
        else
        (0, 0.0, 0.0, 0.0, 0.0, 0L)

        })
          .toDF("endpoint_id", "dur_mean", "dur_stddev", "err_mean", "err_stddev", "create_timestamp")
          //.filter($"endpoint_id" >= 1)
*/
//alerts-NEW
        import com.databricks.spark.avro._
        import scala.collection.immutable.StringOps
        val df = rdd.map(x => {
          val rec = x.value.asInstanceOf[GenericRecord]
          val stats_duration = rec.get("duration").asInstanceOf[GenericRecord]
          val stats_errors = rec.get("errors").asInstanceOf[GenericRecord]
          if(stats_duration != null){
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
            rec.get("timestamp").asInstanceOf[Long]) }
          else
            (new Integer(0), 0.0, 1,1,1,1,1,1,1, 0.0, 1,1,1,1,1,1,1, 0L )
        })
          .map(x => (x._1, x._2, x._3.asInstanceOf[Int], x._4.asInstanceOf[Int], x._5.asInstanceOf[Int], x._6.asInstanceOf[Int], x._7.asInstanceOf[Int], x._8.asInstanceOf[Int], x._9.asInstanceOf[Int]
                    ,x._10, x._11.asInstanceOf[Int], x._12.asInstanceOf[Int], x._13.asInstanceOf[Int], x._14.asInstanceOf[Int], x._15.asInstanceOf[Int], x._16.asInstanceOf[Int], x._17.asInstanceOf[Int], x._18))
          .toDF("endpoint_id", "dur_mean", "dur_zscoreUnder_3", "dur_zscore_2To_3", "dur_zscore_1To_2", "dur_zscore1To_1", "dur_zscore1To2", "dur_zscore2To3", "dur_zscoreAbove3",
                               "err_mean", "err_zscoreUnder_3", "err_zscore_2To_3", "err_zscore_1To_2", "err_zscore1To_1", "err_zscore1To2", "err_zscore2To3", "err_zscoreAbove3", "timestamp")
          .withColumn("timestamp_DT", (col("timestamp").cast(FloatType)/1000).cast(TimestampType))
          //.filter($"endpoint_id" >= 1)
            .withColumn("year", year($"timestamp_DT"))
            //.withColumn("month", lpad(month($"timestamp_DT"), 2, "0"))
          .withColumn("month", month($"timestamp_DT")+2)
          .coalesce(1).write.partitionBy("year", "month").mode("append").avro("C:\\km\\km_big_data\\data\\avro")
        //println("Records count="+ df.count())


        //df.printSchema
        //df
          //.filter($"endpoint_id" === null)
          //.filter($"endpoint_id" >= 252498)
          //.filter(col("duration").rlike("^\\D*")) // ^\\d+
          //.sort($"endpoint_id") //, $"span_created_at".desc)
          //.show(false)

        //})

        //Option#2: Read to DF & show
        //import spark.sqlContext.implicits._
        //val dataDF = spark.read.json(rdd.toDS())
        //dataDF.show(10, false)

        //Option#3: WORKS; read as CustomerRecord to RDD
        /*val recDF = rdd.filter(cr => cr.key() != "Dept")
          .map( cr => {
            try {
              println(getDT() + ": Rec: Key=" + cr.key()+"; Value=[" + cr.value().toString +"]")
              (cr.key(), cr.value().toString)
              //""+cr.key().toString() + "|" + cr.value().toString()+"|"+ cr.partition()+"|"+cr.offset()+"|"+cr.timestamp()
              (cr.key().toString(), cr.value().toString(), cr.partition(), cr.offset(), cr.timestamp() )
            }catch{
              case e: Exception => println("*** Exception.. Continuing to next message ...")
                ("", "", "", "", "")
            }
        }).foreach(println)//.map(x => x.split("|"))
        */
        //.toDF("Key", "val", "Partition", "Offset", "timestamp")
        //.show(2000, false)



      }catch{
        case e: Exception => println("*** Exception.. Continuing to next RDD...")
          e.printStackTrace()
      }

      //Push the result to the Sink Kafka
      //stream.asInstanceOf[CanCommitOffsets].commitAsync(commitOffsetRanges)

      //})
      } else {
        println(getDT() + ": No data received.")
      //logger.error(getDT() + ": No data received")
      }
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
