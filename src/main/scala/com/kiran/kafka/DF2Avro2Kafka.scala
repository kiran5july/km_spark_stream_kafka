package com.kiran.kafka

//import java.util
//import java.{util => ju}

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DF2Avro2Kafka {

  val logger = Logger.getLogger(this.getClass().getName())
  val kafkaTopic = //"rtalab.allstate.is.vision.ingest"
                    "km_test_avro"
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Spark Test 2")
      .setMaster("local[4]")
      .set("spark.io.compression.codec", "snappy") //I think default using lz4

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    val bootstrapServers = //"lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092, lxe0964.allstate.com:9092, lxe0965.allstate.com:9092"
                            "localhost:9092"

    val props = new java.util.Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")
    //props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //classOf[org.apache.kafka.common.serialization.StringDeserializer]
    //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("client.id", UUID.randomUUID().toString())
    //props.put("security.protocol", "SASL_PLAINTEXT")
    //props.put("sasl.kerberos.service.name", "kafka")



    val currentTMS = System.currentTimeMillis()
    println("Current datetime:" + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(currentTMS)))

    var schema: Schema = getSchemaFromFile("C:\\km\\avro\\sample_avro_schema.avsc")
    if( schema == null){
      println("Inavlid path")
    }else{

      println("Schema: " + schema)
      import spark.sqlContext.implicits._
      import org.apache.spark.sql.functions.lit
      //Build DF from JSON data
      val dfJSON = spark.read.json("C:\\km\\avro\\sample_avro_data_as_json.json")
        .withColumn("id", $"clientTag.key")
        .withColumn("email", $"contactPoint.email")
        .withColumn("type", $"contactPoint.type")
        .withColumn("sent", lit("N"))
        .drop("contactPoint", "clientTag", "performCheck")

      dfJSON.show(5, false)
      dfJSON.printSchema()

      import org.apache.avro.specific.SpecificDatumWriter
      import java.io.ByteArrayOutputStream

      println("Iterate DF... ")
      dfJSON.foreachPartition( currentPartition => {

        var producer = new KafkaProducer[String, Array[Byte]](props)
        var schema: Schema = getSchemaFromFile("C:\\km\\avro\\sample_avro_schema.avsc")
        val byteStrm = new ByteArrayOutputStream()
        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(byteStrm, null)
        val writer = new SpecificDatumWriter[GenericRecord](schema)

        val failedRecDF = currentPartition.map(rec =>{
          try {

            println("Rec: " + rec.getAs[String]("id")+";"+ rec.getAs[String]("email")+";"+rec.getAs[String]("type"))
            var avroRecord: GenericRecord = new GenericData.Record(schema)
            avroRecord.put("id", rec.getAs[String]("id"))
            avroRecord.put("email", rec.getAs[String]("email"))
            avroRecord.put("type", rec.getAs[String]("type"))

            // Serialize generic record into byte array

            writer.write(avroRecord, encoder) //VERIFY
            encoder.flush()
            byteStrm.close()
            //val serializedBytes: Array[Byte] = byteStrm.toByteArray()

            producer.send(new ProducerRecord[String, Array[Byte]](kafkaTopic, rec.getAs[String]("id").toString(), byteStrm.toByteArray()))
            (rec.getAs[String]("id"), rec.getAs[String]("email"), rec.getAs[String]("type"), "Success")
          }catch{
            case e: Exception => println("*** Exception *** ")
              e.printStackTrace()

              (rec.getAs[String]("id"), rec.getAs[String]("email"), rec.getAs[String]("type"), "Failed")
          }

        })//.toDF("id", "email", "type", "sent_status")

        import spark.sqlContext.implicits._
        failedRecDF.foreach(println)
      //println("Failed DF..")
      //failedRecDF.show(5, false)
      })


    }




    println("Exit")
  }
    def getDT()
    : String = {

      val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
      return dateFormatter.format(new Date())

      //val today = Calendar.getInstance.getTime
      //return dateFormatter.format(today)
    }

  def getSchemaFromFile(sSchemaFileLoc: String): org.apache.avro.Schema = {

    import org.apache.avro.Schema
    import scala.util.{Failure, Success, Try}

    //Get from avsc file
    //val sSchemaFileLoc = "C:\\km\\km_spark_stream_kafka\\src\\main\\scala\\VisionEvent.avsc"

    Try(scala.io.Source.fromFile(sSchemaFileLoc).mkString ) match {
      case Success(s) => {
        new Schema.Parser().parse(s)
      }
      case Failure(f) => {
        logger.warn("Unable to get schema from file: " + sSchemaFileLoc)
        null
      }
    }

  }
}
