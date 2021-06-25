package batch

import model.Analysis
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import util.RsiCalc

import scala.collection.mutable.ArrayBuffer

class SqlBaseReader(val spark:SparkSession, val url:String) extends Serializable {

  val rsi: RsiCalc = new RsiCalc() //класс для рассчета РСИ

  def tableSize(): Int = {
    val df = spark.read.format("jdbc")
      .option("url", url)
      .option("user", "docker")
      .option("password", "docker")
      .option("query", "SELECT count(*) FROM trader.candles_history")
      .load()

    df.first().get(0).toString.toInt
  }

  def pageable(offset: Int, limit: Int): DataFrame = {
    spark.read.format("jdbc")
      .option("url", url)
      .option("user", "docker")
      .option("password", "docker")
      .option("query", "SELECT open_time, close FROM trader.candles_history ORDER BY 1 OFFSET " + offset +
        " FETCH NEXT " + limit + " ROWS ONLY")
      .load()
  }

  def pageableAllFields(offset: Int, limit: Int): DataFrame = {
    spark.read.format("jdbc")
      .option("url", url)
      .option("user", "docker")
      .option("password", "docker")
      .option("query", "SELECT * FROM trader.candles_history ORDER BY 1 OFFSET " + offset +
        " FETCH NEXT " + limit + " ROWS ONLY")
      .load()
  }

  def dbRead(chunkSize:Int, partitionCount:Int): Dataset[Analysis] ={
    val arr = ArrayBuffer[Analysis]()

    for (i <- 0 to partitionCount) {
      val result:Dataset[Analysis] = handlerData(pageable(i * chunkSize, chunkSize))
      arr ++= result.collect()
    }

    import spark.implicits._
    spark.createDataset(arr)
  }

  def handlerData(df: DataFrame) = {
    import spark.implicits._

    def tt = df.map(elem => {
      println(elem)
      val t = rsi.calculation(elem.get(1).toString.toFloat)
      val a = if(t > 70f) "Перекупленность" else if (t < 30) "Перепроданность" else "Нейтрально"
      val it: Analysis = Analysis(elem.get(0).toString.toLong, t, a)
      it
    })

    tt
  }
}
