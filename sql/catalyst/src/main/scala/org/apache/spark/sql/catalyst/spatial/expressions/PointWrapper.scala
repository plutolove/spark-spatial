package org.apache.spark.sql.catalyst.spatial.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

import org.apache.spark.sql.catalyst.spatial.shapes.Point
import org.apache.spark.sql.catalyst.spatial.ShapeType

/*
 *   Created by plutolove on 08/02/2018.
 */
case class PointWrapper(exps: Seq[Expression]) extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def dataType: DataType = ShapeType

  override def children: Seq[Expression] = exps

  override def eval(input: InternalRow): Any = {
    val coord = exps.map(_.eval(input).asInstanceOf[Double]).toArray
    Point(coord)
  }
}