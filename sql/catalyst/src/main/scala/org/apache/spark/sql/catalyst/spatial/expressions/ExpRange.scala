package org.apache.spark.sql.catalyst.spatial.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.spatial.util.ShapeUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.spatial.shapes.{MBR, Point}

/*
 *   Created by plutolove on 08/02/2018.
 */
case class ExpRange(shape: Expression, low: Expression, high: Expression) extends Predicate with CodegenFallback{
  override def nullable: Boolean = false

  override def toString: String = s" **($shape) IN Rectangle ($low) - ($high)**  "

  override def children: Seq[Expression] = Seq(shape, low, high)

  override def eval(input: InternalRow): Any = {
    val data = ShapeUtil.getShape(shape, input)
    val low_point = low.asInstanceOf[Literal].value.asInstanceOf[Point]
    val high_point = high.asInstanceOf[Literal].value.asInstanceOf[Point]
    require(data.dimensions == low_point.dimensions && low_point.dimensions == high_point.dimensions)
    val mbr = MBR(low_point, high_point)
    mbr.contains(data)
  }
}