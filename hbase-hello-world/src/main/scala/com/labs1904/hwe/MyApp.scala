package com.labs1904.hwe

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Table, Put, Scan, Delete, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}

object MyApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)
  implicit def bytesToString(bytes: Array[Byte]): String = Bytes.toString(bytes)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "hbase01.hourswith.expert:2181")
      connection = ConnectionFactory.createConnection(conf)
      // Example code... change me
      val table = connection.getTable(TableName.valueOf("slewis:users"))

      //val get = new Get(Bytes.toBytes("10000001"))
      //val result = table.get(get)
      //logger.debug(result)
      //val email: String = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail")))
      //val email: String = result.getValue("f1", "mail")
      //logger.debug(email)

      q1_get(table)
/*
      q1(table)
      q1Alt(table)
      q1List(table)

      q2(table)
      q3(table)
      q4(table)
      q5(table)
*/


    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
/*    name, username, sex, favorite_color, mail, and birthdate
*/
    /**
    Challenge #1: What is user=10000001 email address?
      Determine this by using a Get that only returns that user's email address, not their complete column list.
     */
    def q1(t: Table) = {
      val get = new Get(Bytes.toBytes("10000001"))
      val result = t.get(get)
      val mail = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail")))
      logger.debug(mail)
    }

    def q1_get(t: Table): Unit = {
      val get = new Get("10000001")
        .addColumn("f1", "mail")
      val result = t.get(get)
      val mail = result.getValue("f1", "mail")
      logger.debug(mail)
      val email: String = result.getValue("f1", "mail")
      logger.debug(email)
      logger.debug(email)

    }

    def q1Alt(t: Table) = {
      val get = new Get("10000001")
      val result = t.get(get)
      val email: String = result.getValue("f1", "mail")
      logger.debug(email)
    }

    def q1List(t: Table) = {

      val ScalaList = List(Bytes.toBytes("10000001"), Bytes.toBytes("10000002"), Bytes.toBytes("10006001"))

      val ScalaListGet = ScalaList.map(key => new Get(key).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("sex")))

      import scala.collection.JavaConverters._
      val result = t.get(ScalaListGet.asJava)
      result.foreach(x =>
        logger.debug("q1 list result: " + Bytes.toString(x.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sex"))))
      )
    }

    /**
    Challenge #2: Write a new user to your table with:
      Rowkey: 99
      username: DE-HWE
      name: The Panther
      sex: F
      favorite_color: pink
        (Note that favorite_color is a new column qualifier in this table,
        and we are not specifying some other columns every other record has: DOB, email address, etc.)
     */
    def q2(t: Table) = {
      val newMessage = new Put(Bytes.toBytes("99"))
      newMessage.addColumn("f1", "username", "DE-HWE")
      newMessage.addColumn("f1", "name", "The Panther")
      newMessage.addColumn("f1", "sex", "F")
      newMessage.addColumn("f1", "favorite_color", "pink")
      t.put(newMessage)

      val get = new Get("99")
      val result = t.get(get)
      val favorite_color = result.getValue("f1", "favorite_color")
      logger.debug("q2: " + favorite_color)

    }

    /**
    Challenge #3: How many user IDs exist between 10000001 and 10006001? (Not all user IDs exist, so the answer is not
     */
    def q3(t: Table) = {
      val scan = new Scan().withStartRow("10000001").withStopRow("10006001")

      val resultScanner = t.getScanner(scan)

      import scala.collection.JavaConverters._
      logger.debug("lab 3 size: " + resultScanner.iterator().asScala.size)
      /*
      //https://stackoverflow.com/questions/1072784/how-can-i-convert-a-java-iterable-to-a-scala-iterable
      // The Scala community has arrived at a broad consensus that JavaConverters is good, and JavaConversions is bad,
      // because of the potential for spooky-action-at-a-distance
      import scala.collection.JavaConverters._
      val myJavaIterable = someExpr()
      val myScalaIterable = myJavaIterable.asScala

      //Starting Scala 2.13, package scala.jdk.CollectionConverters replaces deprecated packages scala.collection.JavaConverters/JavaConversions:
      import scala.jdk.CollectionConverters._
      // val javaIterable: java.lang.Iterable[Int] = Iterable(1, 2, 3).asJava
      javaIterable.asScala
      // Iterable[Int] = List(1, 2, 3)
       */
    }

    /**
    Challenge #4: Delete the user with ID = 99 that we inserted earlier.
     */
    def q4(t: Table) = {
      val getPre = new Get("99")
      val resultPre = t.get(getPre)
      val favoriteColorPre = resultPre.getValue("f1", "favorite_color")
      logger.debug("pre delete: " + favoriteColorPre)

      val delete = new Delete("99")
      t.delete(delete)

      val getPost = new Get("99")
      val resultPost = t.get(getPost)
      val favoriteColorPost = resultPost.getValue("f1", "favorite_color")
      logger.debug("post delete: " + favoriteColorPost)

    }

    /**
    Challenge #5: There is also an operation that returns multiple results in a single HBase "Get" operation.
      Write a single HBase call that returns the email addresses for the following 5 users:
        9005729, 500600, 30059640, 6005263, 800182 (Hint: Look at the Javadoc for "Table")
     */
    def q5(t: Table) = {
      val listGet: List[Get] = List("9005729", "500600", "30059640", "6005263", "800182")
        .map(key => new Get(key).addColumn("f1", "mail"))

      import scala.collection.JavaConverters._
      val results: Array[Result] = t.get(listGet.asJava)
      results.foreach(x =>
        logger.debug("result: " + x.getValue("f1", "mail"))
      )

    }

  }
}
