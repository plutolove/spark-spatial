package org.apache.spark.examples.sql.spatial

import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.log4j.{Logger, Level}

/*
 *   Created by plutolove on 08/02/2018.
 */

object Range {
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
    val datapoints = points.toDS()
    datapoints.createOrReplaceTempView("b")
    datapoints.createIndex("rtree", "RtreeForData",  Array("x", "y") )
    datapoints.range(Array("x", "y"), Array(0, 0), Array(10, 10)).show()
  }
}