package stream

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import stream.service.{HandlerDataSql, KafkaCallback, KafkaHelper}

object Producer {

  val spark: SparkSession = SparkSession.builder()
    .appName("Producer")
    .master("local")
    .getOrCreate()

  val url = "jdbc:postgresql://localhost:5432/binance"
  val data = new HandlerDataSql(spark, url)

  private val TOPIC = "ETHUSDT"
  private val PARTITION_COUNT = 1

  def main(args: Array[String]): Unit = {
    val producer: KafkaProducer[String, String] = KafkaHelper.createProducer()

    val callback = new KafkaCallback

    while (true) {
      val dt = data.getData //данные

      dt.foreach(el => {
        producer.send(new ProducerRecord(TOPIC, (el.openTime + "," + el.close)), callback)
      })
      Thread.sleep(2000)
    }
    producer.close()
  }

}
