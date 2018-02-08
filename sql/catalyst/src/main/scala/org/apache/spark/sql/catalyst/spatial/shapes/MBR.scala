package org.apache.spark.sql.catalyst.spatial.shapes

import org.apache.spark.sql.catalyst.spatial.ShapeType
import org.apache.spark.sql.types.SQLUserDefinedType

/*
 *   Created by plutolove on 08/02/2018.
 *   Minimum Bounding Box
 */
@SQLUserDefinedType(udt = classOf[ShapeType])
case class MBR(low: Point, high: Point) extends Shape {
  require(low.dimensions == high.dimensions)
  require(low <= high)
  override val dimensions: Int = low.dimensions

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
    }
  }

  def minDist(p: Point): Double = {
    require(low.coord.length == p.coord.length)
    var ans = 0.0
    for (i <- p.coord.indices) {
      if (p.coord(i) < low.coord(i)) {
        ans += (low.coord(i) - p.coord(i)) * (low.coord(i) - p.coord(i))
      } else if (p.coord(i) > high.coord(i)) {
        ans += (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i))
      }
    }
    Math.sqrt(ans)
  }

  def contains(point: Shape): Boolean = {
    require(dimensions == point.dimensions)
    point match {
      case p: Point =>
        for (i <- p.coord.indices) {
          if (p.coord (i) > high.coord (i) || p.coord (i) < high.coord (i) ) return false
        }
        true
      case _ => false
    }
  }

  override def toString: String = "MBR(" + low.toString + "," + high.toString + ")"
}