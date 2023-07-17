package com.labs1904.hwe

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class WordCount(word: String, count: Int)

object WordCountBatchApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "WordCountBatchApp_slewis"

  def main(args: Array[String]): Unit = {
    logger.info(s"$jobName starting...")
    try {
      val spark = SparkSession.builder()
        .appName(jobName)
        .config("spark.sql.shuffle.partitions", "3")
        .master("local[2]")
        .getOrCreate()

      import spark.implicits._
      val sentences = spark.read.csv("src/main/resources/sentences.txt").as[String]
      //sentences.printSchema
      //sentences.show(false)

      val counts = sentences.flatMap(row => splitSentenceIntoWords(row))
        .map(word => WordCount(word, 1))
        .groupBy(col("word"))
        .count()
        .sort(col("count").desc)

      counts.foreach(wordCount=>println(wordCount))
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  // HINT: you may have done this before in Scala practice...
  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.toLowerCase.replaceAll("[^a-z]", ""))
  }

}
