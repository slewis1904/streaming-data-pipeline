package com.labs1904.hwe.producers

import com.labs1904.hwe.util.Util
import com.labs1904.hwe.util.Util.getScramAuthString
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object SimpleProducer {
  // Set constants
  // TODO: this is configured to use kafka running locally, change it to your cluster
  val BootstrapServer: String = Util.kafkaConnection("hwe_bootstrap_server")
  val username: String = Util.kafkaConnection("hwe_username")
  val password: String = Util.kafkaConnection("hwe_password")
  val trustStore: String = "src/main/resources/kafka.client.truststore.jks"
  val Topic: String = "question-1-output"

  def main(args: Array[String]): Unit = {
    // Create Kafka Producer
    val properties = getProperties(BootstrapServer)
    val producer = new KafkaProducer[String, String](properties)
    val messageToSend = "Change Me"

    val record = new ProducerRecord[String, String](Topic, messageToSend)

    producer.send(record)

    producer.close()
  }

  def getProperties(bootstrapServer: String): Properties = {
    // Set Properties to be used for Kafka Producer
    val properties = new Properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    properties.put("security.protocol", "SASL_SSL")
    properties.put("sasl.mechanism", "SCRAM-SHA-512")
    properties.put("ssl.truststore.location", trustStore)
    properties.put("sasl.jaas.config", getScramAuthString(username, password))
    properties
  }
}
