package stream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI

object App {

  val spark: SparkSession = SparkSession.builder()
    .appName("BaseOperations")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    reader()
  }

  def reader(): Unit ={

    val hdfs = Hdfs("hdfs://namenode:9000/")
    val path = "/ods/stream"

    var cc = 0
    getDirectory(path)(hdfs).foreach(el => {

      println(el)

      if(el.isFile && isParquet(el.getPath)) {
        println(">>>>>  READ DATA  <<<<< ")
        val parquetDF = spark.read.orc(el.getPath.toString)
        cc += parquetDF.collect().size
        parquetDF.show()
      }
    })

    println("elem count: "+cc)

    //hdfs.deleteOnExit(new Path(path))

    hdfs.close()

  }

  def Hdfs(urlHdfs: String): FileSystem = {
    FileSystem.get(new URI(urlHdfs), new Configuration())
  }

  def isParquet(path: Path): Boolean = {
    path.getName.matches(".*orc.*")
  }

  def getDirectory(path: String)(hdfs: FileSystem): Array[FileStatus] = {
    hdfs.listStatus(new Path(path))
  }
}
