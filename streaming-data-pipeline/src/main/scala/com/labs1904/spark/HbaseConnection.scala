package com.labs1904.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseConnection {
  private var connection: Connection = null
  private var numCreated: Int = 0
  private var numRetrieved: Int = 0

  def getOrCreate(): Connection = this.synchronized {
    numRetrieved += 1
    if (connection == null) {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "hbase01.hourswith.expert:2181")
      connection = ConnectionFactory.createConnection(conf)
      numCreated += 1
    }
    println(s"getOrCreate: created $numCreated, retrieved $numRetrieved")
    connection
  }
}