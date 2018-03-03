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
      case mbr: MBR => mbr.minDist(this)
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

  override def intersects(other: Shape): Boolean = {
    other match {
      case p: Point => p == this
      case mbr: MBR => intersects(mbr)
    }
  }

  def intersects(other: MBR): Boolean = {
    require(low.coord.length == other.low.coord.length)
    for (i <- low.coord.indices)
      if (low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i)) {
        return false
      }
    true
  }

  def contains(point: Shape): Boolean = {
    require(dimensions == point.dimensions)
    point match {
      case p: Point =>
        for (i <- p.coord.indices) {
          if (p.coord (i) > high.coord (i) || p.coord (i) < low.coord (i) ) return false
        }
        true
      case _ => false
    }
  }

  def area: Double = low.coord.zip(high.coord).map(x => x._2 - x._1).product

  def calcRatio(query: MBR): Double = {
    val intersect_low = low.coord.zip(query.low.coord).map(x => Math.max(x._1, x._2))
    val intersect_high = high.coord.zip(query.high.coord).map(x => Math.min(x._1, x._2))
    val diff_intersect = intersect_low.zip(intersect_high).map(x => x._2 - x._1)
    if (diff_intersect.forall(_ > 0)) 1.0 * diff_intersect.product / area
    else 0.0
  }

  override def toString: String = "MBR(" + low.toString + "," + high.toString + ")"
}