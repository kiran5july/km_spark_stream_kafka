package com.kiran.kafka

//import java.util
//import java.{util => ju}

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.{SparkConf, SparkContext}

object KakfaConsumerTopicsList {

  import java.util.Properties

  def main(args: Array[String]): Unit = {
    val bootstrapServers = "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092"
                          //"localhost:9092" //"lxe0730.allstate.com:9092,lxe0731.allstate.com:9092,lxe0731.allstate.com:9092"

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "KM_Consumer")
    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.kerberos.service.name", "kafka")

    val kafkaConsumer = new KafkaConsumer[String, String](props)

    //View the topics list
    val topics = kafkaConsumer.listTopics()

    val itr = topics.keySet().iterator()
    while(itr.hasNext())
      println("Topic: " + itr.next())


    println("Exit")
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
