package com.kiran.kafka

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import _root_.kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Milliseconds}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
//import org.apache.spark.sql.hive.HiveContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, CanCommitOffsets, OffsetRange}

object SparkStreamKafka010TopicDemo {

  def main(args: Array[String]) {

    //Using SparkSession & SparkContext
    //val spark = SparkSession
    //  .builder()
    //  .master("local[20]")
    //  .appName("Spark Streaming data from Kafka")
     // .getOrCreate()

    //spark.sparkContext.setLogLevel("ERROR")

    //spark.conf.getAll.mkString("\n").foreach(print)

    //val ssc = new StreamingContext(spark.sparkContext.getConf, Seconds(5))
    //Above gives Error: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true

    //val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val conf = new SparkConf().setAppName("Spark Streaming data from Kafka").setMaster("local[10]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(10))
    //StreamingContext without SparkContext
    //val ssc = new StreamingContext("local[2]", "Spark Streaming data from Kafka", Seconds(30))

    //ssc.checkpoint("_checkpointing")

    val kafkaParams: Map[String, Object] = Map(
      //"zookeeper.connect" -> "localhost:2181",
      "bootstrap.servers" -> "localhost:9092"   //org.apache.spark.SparkException: Must specify metadata.broker.list or bootstrap.servers
      //"metadata.broker.list" -> "localhost:9092",
      ,"group.id" -> "kafkaStreamData2"
      ,"key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer]
      ,"value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer]
      ,"zookeeper.session.timeout.ms" ->	"500"
      ,"zookeeper.sync.time.ms" -> "250"
      ,"auto.commit.interval.ms" ->	"1000"
      ,"auto.offset.reset" -> "earliest" //latest, earliest, none
      ,"spark.streaming.kafka.maxRatePerPartition" -> ""
      ,"enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("kafkatopic") //This topic has PartitionCount:10

    //process each RDD
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD(
      rdd =>  {
        if (!rdd.isEmpty()) {
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          for (o <- offsetRanges) {
            println(s"Topic=${o.topic}; Partition=${o.partition}; Offset (${o.fromOffset} - ${o.untilOffset})")
          }

          rdd.foreach(println)


          val prodSalesTotal = rdd.map(rec => (rec.key, rec.value)).map(_._2)
            .filter(rec => rec.split(",").length == 7)
            //filter lines if qty>0
            .filter(rec => rec.split(",")(5).toInt > 0)
            //map to curr_datetime, prod_id & qty
            .map(rec => (rec.split(",")(1) + "," + rec.split(",")(4), rec.split(",")(5).toInt))
            .reduceByKey(_ + _)
            //.map(x => System.currentTimeMillis().toString()+","+x._1+","+x._2)
            .map(x => x._1 + "," + x._2)

          //print("Partitions count: "+x.partitions.length)
          println("Total rows count: " + rdd.count())
          //rdd.take(15).foreach(println)
          println("Result rows count:" + prodSalesTotal.count())
          prodSalesTotal.take(50).foreach(println)

          //see messages count by partition
          rdd.map(rec => (rec.partition, 1)).reduceByKey(_ + _).foreach(println)
          //stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }else
          println("No data received at " + getDT() )
      })

    ssc.start()
    ssc.awaitTermination()


  }
  def getDT(): String ={

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)

  }
}
