package stream.service

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import java.util.Properties

object KafkaHelper {

  def createProducer(): KafkaProducer[String, String] ={
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    producer
  }

  def createConsumer(): KafkaConsumer[String,String] ={
    val props = new Properties
    props.setProperty("bootstrap.servers", "localhost:29092")
    props.setProperty("group.id", "testGroup")
    props.setProperty("enable.auto.commit", "false")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)
    consumer
  }

}
