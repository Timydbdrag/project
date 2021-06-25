package batch

import model.Analysis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.net.URI

object App {

  val spark: SparkSession = SparkSession.builder()
    .appName("BaseOperations")
    .master("local[*]")
    .getOrCreate()

  val url = "jdbc:postgresql://localhost:5432/binance"
 // val rsi: RsiCalc = new RsiCalc() //класс для рассчета РСИ
  val db:SqlBaseReader =new SqlBaseReader(spark,url)

  def main(args: Array[String]): Unit = {
    println("Start...")

    val chunkSize: Int = 1000  //количество строк за один запрос
    val partitionCount: Int = db.tableSize() / chunkSize //количество партиций

    //val ds = db.dbRead(chunkSize, partitionCount)  //read data from SQL

    //writer(ds,"analysis") //write on hdfs

    reader()  //read hdfs

    println("Done!")
  }


  def reader(): Unit ={

    val hdfs = Hdfs("hdfs://namenode:9000/")
    val path = "/ods"

    var cc = 0
    getDirectory(path)(hdfs).foreach(el => {
      if(el.isDirectory) {
        getDirectory(path+"/"+el.getPath.getName)(hdfs).foreach(ell => {
          if (ell.isFile && isParquet(ell.getPath)) {
            val parquetDF = spark.read.parquet(ell.getPath.toString)
            cc += parquetDF.collect().size
            parquetDF.show()
            //parquetDF.take(100).foreach(println)
          }
          println(ell)
        })
      }
    })

    println("elem count: "+cc)

  //  hdfs.deleteOnExit(new Path(writePath))

    hdfs.close()

  }

  def writer(ds: Dataset[Analysis], catalog:String) = {
    ds
      .write
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://namenode:9000/ods/"+catalog)
  }

  def isParquet(path: Path): Boolean = {
    path.getName.matches(".*parquet.*")
  }

  def Hdfs(urlHdfs: String): FileSystem = {
    FileSystem.get(new URI(urlHdfs), new Configuration())
  }

  def getDirectory(path: String)(hdfs: FileSystem): Array[FileStatus] = {
    hdfs.listStatus(new Path(path))
  }

}
