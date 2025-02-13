package com.labs1904.hwe.util

import java.util.Properties
import scala.io.Source
import scala.collection.JavaConverters._

object Util {

  private val path = "connection.properties"
  private val url = getClass.getResource(path)
  private val properties: Properties = new Properties()
  if (url != null) {
    val source = Source.fromURL(url)
    properties.load(source.bufferedReader())
  }
  else {
    println(s"properties file cannot be loaded at path $path")
    //logger.error("properties file cannot be loaded at path " +path)
    //throw new FileNotFoundException("Properties file cannot be loaded);
  }
  val table = properties.getProperty("hbase_table_name")
  val zquorum = properties.getProperty("localhost")
  val port = properties.getProperty("2181")


  def getScramAuthString(username: String, password: String) = {
   s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
  }

  val kafkaConnection: Map[String, String] = Map[String, String] (
    "hwe_bootstrap_server" -> "b-3-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-2-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-1-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196",
    "hwe_username" -> "1904labs",
    "hwe_password" -> "1904labs"
  )

  def mapNumberToWord(number: Int) : String = {
    numberToWordMap(number)
  }

  val numberToWordMap: Map[Int, String] = Map[Int, String](
    1->"One"
    ,2->"Two"
    ,3->"Three"
    ,4->"Four"
    ,5->"Five"
    ,6->"Six"
    ,7->"Seven"
    ,8->"Eight"
    ,9->"Nine"
    ,10->"Ten"
    ,11->"Eleven"
    ,12->"Twelve"
    ,13->"Thirteen"
    ,14->"Fourteen"
    ,15->"Fifteen"
    ,16->"Sixteen"
    ,17->"Seventeen"
    ,18->"Eighteen"
    ,19->"Nineteen"
    ,20->"Twenty"
    ,21->"Twenty-one"
    ,22->"Twenty-two"
    ,23->"Twenty-three"
    ,24->"Twenty-four"
    ,25->"Twenty-five"
    ,26->"Twenty-six"
    ,27->"Twenty-seven"
    ,28->"Twenty-eight"
    ,29->"Twenty-nine"
    ,30->"Thirty"
    ,31->"Thirty-one"
    ,32->"Thirty-two"
    ,33->"Thirty-three"
    ,34->"Thirty-four"
    ,35->"Thirty-five"
    ,36->"Thirty-six"
    ,37->"Thirty-seven"
    ,38->"Thirty-eight"
    ,39->"Thirty-nine"
    ,40->"Forty"
    ,41->"Forty-one"
    ,42->"Forty-two"
    ,43->"Forty-three"
    ,44->"Forty-four"
    ,45->"Forty-five"
    ,46->"Forty-six"
    ,47->"Forty-seven"
    ,48->"Forty-eight"
    ,49->"Forty-nine"
    ,50->"Fifty"
    ,51->"Fifty-one"
    ,52->"Fifty-two"
    ,53->"Fifty-three"
    ,54->"Fifty-four"
    ,55->"Fifty-five"
    ,56->"Fifty-six"
    ,57->"Fifty-seven"
    ,58->"Fifty-eight"
    ,59->"Fifty-nine"
    ,60->"Sixty"
    ,61->"Sixty-one"
    ,62->"Sixty-two"
    ,63->"Sixty-three"
    ,64->"Sixty-four"
    ,65->"Sixty-five"
    ,66->"Sixty-six"
    ,67->"Sixty-seven"
    ,68->"Sixty-eight"
    ,69->"Sixty-nine"
    ,70->"Seventy"
    ,71->"Seventy-one"
    ,72->"Seventy-two"
    ,73->"Seventy-three"
    ,74->"Seventy-four"
    ,75->"Seventy-five"
    ,76->"Seventy-six"
    ,77->"Seventy-seven"
    ,78->"Seventy-eight"
    ,79->"Seventy-nine"
    ,80->"Eighty"
    ,81->"Eighty-one"
    ,82->"Eighty-two"
    ,83->"Eighty-three"
    ,84->"Eighty-four"
    ,85->"Eighty-five"
    ,86->"Eighty-six"
    ,87->"Eighty-seven"
    ,88->"Eighty-eight"
    ,89->"Eighty-nine"
    ,90->"Ninety"
    ,91->"Ninety-one"
    ,92->"Ninety-two"
    ,93->"Ninety-three"
    ,94->"Ninety-four"
    ,95->"Ninety-five"
    ,96->"Ninety-six"
    ,97->"Ninety-seven"
    ,98->"Ninety-eight"
    ,99->"Ninety-nine"
    ,100->"One-hundred"
  )
}
