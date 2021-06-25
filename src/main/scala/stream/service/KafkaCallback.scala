package stream.service

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class KafkaCallback extends Callback {
  def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
    if (e != null) {
      println("Error while producing message to topic :" + recordMetadata)
      e.printStackTrace()
    }
    else {
      println("sent message to topic:"+recordMetadata.topic+" partition:"+recordMetadata.partition
        +"  offset: " + recordMetadata.offset)
    }
  }
}