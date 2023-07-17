package com.labs1904.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipelineSlewis"

  val hdfsUrl = "hdfs://hbase01.hourswith.expert:8020"
  val bootstrapServers = "b-3-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-2-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-1-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196"
  val hbaseZK = "hbase01.hourswith.expert:2181"
  val username = "1904labs"
  val password = "1904labs"
  val hdfsUsername = "slewis"

  //Use this for Windows
  //val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  val trustStore: String = "src/main/resources/kafka.client.truststore.jks"

  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)
  implicit def bytesToString(bytes: Array[Byte]): String = Bytes.toString(bytes)

  def main(args: Array[String]): Unit = {
    try {
      val spark: SparkSession = SparkSession.builder()
        .config("spark.sql.shuffle.partitions", "3")
//        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
//        .config("spark.hadoop.fs.defaultFS", hdfsUrl)
        .appName(jobName)
        .master("local[3]")
        .getOrCreate()

      import spark.implicits._

      val ds: Dataset[String] = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "20")
        .option("startingOffsets","earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.ssl.truststore.location", trustStore)
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password))
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      /*
        Get messages from Kafka
       */
      val rawReviews: Dataset[Review] = ds.map(csvLine => {
        val csvArray: Array[String] = csvLine.split("\t")
        Review(marketplace = csvArray(0),
          customer_id = csvArray(1),
          review_id = csvArray(2),
          product_id = csvArray(3),
          product_parent = csvArray(4),
          product_title = csvArray(5),
          product_category = csvArray(6),
          star_rating = csvArray(7),
          helpful_votes = csvArray(8),
          total_votes = csvArray(9),
          vine = csvArray(10),
          verified_purchase = csvArray(11),
          review_headline = csvArray(12),
          review_body = csvArray(13).take(30),
          review_date = csvArray(14)
        )
      })


      /*
        Enrich Review by looking up ID in HBase
       */
      val enrichedReviews: Dataset[EnrichedReview] = rawReviews.mapPartitions(partition => {
        val connection = HbaseConnection.getOrCreate()

        val table = connection.getTable(TableName.valueOf("slewis:users"))

        val iter = partition.map(r => {
          val get = new Get(r.customer_id).addFamily("f1")
          val result = table.get(get)
          val u: User = User(
            name = result.getValue("f1", "name"),
            birthdate = result.getValue("f1", "birthdate"),
            mail = result.getValue("f1", "mail"),
            sex = result.getValue("f1", "sex"),
            username = result.getValue("f1", "username"))

          val name: String = result.getValue("f1", "name")
          val birthdate: String = result.getValue("f1", "birthdate")

          EnrichedReview(r.marketplace, r.customer_id, r.review_id, r.product_id, r.product_parent, r.product_title,
            r.product_category, r.star_rating, r.helpful_votes, r.total_votes, r.vine, r.verified_purchase,
            r.review_headline, r.review_body, r.review_date,
            u.name, u.birthdate, u.mail, u.sex, u.username)

        }).toList.iterator

//        connection.close()

        iter
      })

/*
      // Write output to console
      val query = enrichedReviews.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()
*/

      // Write output to HDFS
      val query = enrichedReviews.writeStream
        .outputMode(OutputMode.Append())
        .format("json")
        .option("path", s"/user/${hdfsUsername}/reviews_json")
        .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint")
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
}
