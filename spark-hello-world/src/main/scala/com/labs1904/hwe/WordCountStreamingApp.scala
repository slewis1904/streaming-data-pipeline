package com.labs1904.hwe

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * Spark Structured Streaming app
 */
object WordCountStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "WordCountStreamingApp_slewis"
  // TODO: define the schema for parsing data from Kafka

  val bootstrapServer : String = "b-3-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-2-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-1-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196"
  val username: String = "1904labs"
  val password: String = "1904labs"
  val Topic: String = "word-count"

  //Use this for Windows
  //val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  val trustStore: String = "src/main/resources/kafka.client.truststore.jks"

  def main(args: Array[String]): Unit = {
    logger.info(s"$jobName starting...")

    try {
      val spark = SparkSession.builder()
        .appName(jobName)
        .config("spark.sql.shuffle.partitions", "3")
        .master("local[2]")
        .getOrCreate()

      import spark.implicits._

      val sentences = spark
        .readStream
        .format("kafka")
        .option("maxOffsetsPerTrigger", 10)
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("subscribe", "word-count")
        .option("kafka.acks", "1")
        .option("kafka.key.serializer", classOf[StringSerializer].getName)
        .option("kafka.value.serializer", classOf[StringSerializer].getName)
        .option("startingOffsets","earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.ssl.truststore.location", trustStore)
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password))
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      sentences.printSchema
      //root
      //|-- value: string (nullable = true)

      val counts = sentences
        .flatMap(row => splitSentenceIntoWords(row))
        .map(word => WordCount(word, 1))
        .groupBy(col("word"))
        .count()
        .sort(col("count").desc)
        .limit(10)

      val query = counts.writeStream
//        .outputMode(OutputMode.Append())
        .outputMode(OutputMode.Complete()) // Changed from Append so we don't have to do watermarks
        .format("console")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def getScramAuthString(username: String, password: String) = {
    s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.toLowerCase.replaceAll("[^a-z]", ""))
  }

}
