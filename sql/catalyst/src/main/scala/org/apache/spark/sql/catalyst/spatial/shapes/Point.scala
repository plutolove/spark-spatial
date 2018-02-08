package org.apache.spark.sql.catalyst.spatial.shapes

import org.apache.spark.sql.catalyst.spatial.ShapeType
import org.apache.spark.sql.types.SQLUserDefinedType

/*
 *   Created by plutolove on 08/02/2018.
 */
@SQLUserDefinedType(udt = classOf[ShapeType])
case class Point(coord: Array[Double]) extends Shape {
  override val dimensions :Int = coord.length

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: MBR => mbr.minDist(this)
    }
  }

  def minDist(other: Point): Double = {
    require(coord.length == other.coord.length)
    var ans = 0.0
    for (i <- coord.indices)
      ans += (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
    Math.sqrt(ans)
  }

  def ==(other: Point): Boolean = {
    other match {
      case p: Point =>
        if (p.coord.length != coord.length) false
        else {
          for (i <- coord.indices)
            if (coord(i) != p.coord(i)) return false
          true
        }
      case _ => false
    }
  }

  def <=(other: Point): Boolean = {
    for (i <- coord.indices)
      if (coord(i) > other.coord(i)) return false
    true
  }

}