package com.kiran.kafka

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.kafka.schemaregistry.client.rest.RestService

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._


object SchemaRegistry extends StrictLogging {

  /**
    * Get a schema for a given subject
    *
    * @param url The url of the schema registry
    * @param subject The subject to het the schema for
    * @return The schema for the subject
    * */
  def getSchema(url : String, subject : String) : String = {
    val registry = new RestService(url)

    Try(registry.getLatestVersion(subject).getSchema) match {
      case Success(s) => {
        logger.info(s"Found schema for $subject")
        s
      }
      case Failure(f) => {
        logger.warn("Unable to connect to the Schema registry. An attempt will be made to create the table" +
          " on receipt of the first records.")
        ""
      }
    }
  }

  /**
    * Get a list of subjects from the registry
    *
    * @param url The url to the schema registry
    * @return A list of subjects/topics
    * */
  def getSubjects(url: String) : List[String] = {
    val registry = new RestService(url)
    val schemas: List[String] = Try(registry.getAllSubjects.asScala.toList) match {
      case Success(s) => s
      case Failure(f) => {
        logger.warn("Unable to connect to the Schema registry. An attempt will be made to create the table" +
          " on receipt of the first records.")
        List.empty[String]
      }
    }

    schemas.foreach(s=>logger.info(s"Found schemas for $s"))
    schemas
  }

  def main(args: Array[String]): Unit = {

    val sSchemaRegistryURL = "http://lxe0961.allstate.com:8081"
    val sSubjectName = //"rtalab.allstate.is.vision.ingest-value"
                        //"rtalab.allstate.is.vision.alerts-value"
                        "rtalab.allstate.is.vision.ingest-value"

    println("Subjects: %s".format(  SchemaRegistry.getSubjects(sSchemaRegistryURL)    ))

    println("Schema %s".format(        SchemaRegistry.getSchema(sSchemaRegistryURL, sSubjectName)     ))
  }

}
