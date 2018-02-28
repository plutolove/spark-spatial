package org.apache.spark.sql.catalyst.spatial.shapes

import org.apache.spark.sql.catalyst.spatial.ShapeType
import org.apache.spark.sql.types.SQLUserDefinedType

/*
 *   Created by plutolove on 08/02/2018.
 */

@SQLUserDefinedType(udt = classOf[ShapeType])
abstract class Shape extends Serializable {
  val dimensions: Int
  def minDist(other: Shape): Double


  def intersects(other: Shape): Boolean
  /*
  def getMBR: MBR
  */
}