# Apache Spark
[![Build Status](https://travis-ci.org/plutolove/spark-spatial.svg?branch=master)](https://travis-ci.org/plutolove/spark-spatial)

本项目基于Apache Spark，在Dataset中添加了range和knn查询，同时支持RTree索引，加速range和knn查询。
### Develop Note
* [Add your own query operation in spark](http://plutolove.hatenablog.com/entry/2018/02/27/171804)
* [RTree index speed up Range query in SparkSQL](http://plutolove.hatenablog.com/entry/2018/03/04/173207)
### Example
```scala
import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.log4j.{Logger, Level}
object Main {
  case class PointData(x: Double, y: Double, z: Double, other: String)
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkSession")
      .getOrCreate()
    import sparkSession.implicits._
    var points = Seq[PointData]()
    for(i <- 0 until 3000) {
      points = points :+ PointData(Random.nextInt()%30, Random.nextInt()%30, Random.nextInt()%30, "point: "+i.toString)
    }
    val pointsList = points.toDS()
    pointsList.createOrReplaceTempView("b")
    pointsList.createIndex("rtree", "RtreeForData",  Array("x", "y") )
    pointsList.range(Array("x", "y"), Array(0, 0), Array(10, 10)).show()
    pointsList.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)
}
```