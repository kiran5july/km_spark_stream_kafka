package com.kiran.kafka

//import java.util
//import java.{util => ju}

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object JSON2Avro {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Spark Test 2")
      .setMaster("local[4]")
      .set("spark.io.compression.codec", "snappy") //I think default using lz4

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate

    val sc = spark.sparkContext
    //sc.setLogLevel("INFO")

    import spark.implicits._

    val currentTMS = System.currentTimeMillis()
    println("Current datetime:" + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(currentTMS)))

    val dfJSON = spark.read.json("C:\\km\\avro\\sample_avro_data_as_json.json")
    dfJSON.show(5, false)
    dfJSON.printSchema()

    println("Writing to Avro...")
    dfJSON.write.format("com.databricks.spark.avro").save("C:\\km\\km\\avro_data")

    println("Reading Avro data...")
    import com.databricks.spark.avro._
    val dfAvro = spark.read.avro("C:\\km\\avro\\avro_data")
    dfAvro.show(5, false)


    println("Exit")
  }
    def getDT()
    : String = {

      val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
      return dateFormatter.format(new Date())

      //val today = Calendar.getInstance.getTime
      //return dateFormatter.format(today)
    }
}
