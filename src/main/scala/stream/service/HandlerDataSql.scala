package stream.service

import batch.SqlBaseReader
import model.{Analysis, CandlesData}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

class HandlerDataSql(spark:SparkSession, url:String, private var count:Int = 0) {

  val db:SqlBaseReader =new SqlBaseReader(spark,url)


  def getData: List[CandlesData] = {

    val chunkSize: Int = 1000  //количество строк за один запрос
    val partitionCount: Int = db.tableSize() / chunkSize //количество партиций

    var res = List[CandlesData]()

    if(count <= partitionCount) {

    def result = db.pageableAllFields( count * chunkSize, chunkSize)

      count +=1

      import spark.implicits._

      res ++= result.map(el => CandlesData(
        el.get(0).toString.toLong,
        el.get(1).toString,
        el.get(2).toString,
        el.get(3).toString,
        el.get(4).toString)
      ).collect().toList

    }

   res
  }

}
