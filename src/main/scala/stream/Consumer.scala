package stream

import model.Analysis
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import util.RsiCalc

import scala.collection.mutable.ArrayBuffer

object Consumer {
  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("kafka stream")
      .getOrCreate()
  }

  val TOPIC = "ETHUSDT"
  val rsi: RsiCalc = new RsiCalc() //класс для рассчета РСИ

  val buffer = new ArrayBuffer[Analysis](1100)

  def main(args: Array[String]): Unit = {
    startConsumer()
  }


  private def startConsumer(): Unit = {

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", TOPIC)
      .load()

    val res2 = df.selectExpr("CAST(value AS STRING)").as[String]

    def arr2 = res2.map(
      _.replace("\"", "")
        .split(","))
      .map(e => (e(0).trim, e(1).trim))
      .toDF("openTime", "closePrice")

    val res = handlerData(arr2)

    res.writeStream
      .outputMode("append")
      .format("orc")
      .option("path", "hdfs://namenode:9000/ods/stream")
      .option("checkpointLocation", "hdfs://namenode:9000/ods/stream/checkPoints")
      .trigger(ProcessingTime(5000))
      .start()
      .awaitTermination()

  }

  def batteryAndSave(ds: Dataset[Analysis]): Unit = {

    import spark.implicits._

    if (buffer.size < 1000) {
      buffer ++= ds.collect()
      println("add data in buffer")
    }

    if (buffer.size >= 1000) {
      try {
        spark.createDataset(buffer)
          .repartition(1)
          .write
          .mode("append")
          .option("header", "true")
          .save(path = "hdfs://namenode:9000/ods/stream/data.csv")

      } catch {
        case e: Exception => e.printStackTrace()
      }

      buffer.clear()

    }

  }

  def handlerData(df: DataFrame) = {

    import spark.implicits._

    val tt = df.map(elem => {
      println(elem)
      val t = rsi.calculation(elem.get(1).toString.toFloat)
      val a = if (t > 70f) "Перекупленность" else if (t < 30f) "Перепроданность" else "Нейтрально"
      val it: Analysis = Analysis(elem.get(0).toString.toLong, t, a)
      it
    })

    tt
  }

}
