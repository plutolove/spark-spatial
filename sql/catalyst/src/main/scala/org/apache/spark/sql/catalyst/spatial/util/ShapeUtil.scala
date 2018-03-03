package org.apache.spark.sql.catalyst.spatial.util

import org.apache.spark.sql.catalyst.spatial.{ShapeSerializer, ShapeType}
import org.apache.spark.sql.catalyst.spatial.expressions.PointWrapper
import org.apache.spark.sql.catalyst.spatial.shapes.{Point, Shape}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeArrayData}

/*
 *   Created by plutolove on 08/02/2018.
 */
object ShapeUtil {
  //get shape from InternalRow with schema
  def getShape(expression: Expression, schema: Seq[Attribute], input: InternalRow): Shape = {
    if (!expression.isInstanceOf[PointWrapper] && expression.dataType.isInstanceOf[ShapeType]) {
      ShapeSerializer.deserialize(BindReferences.bindReference(expression, schema)
        .eval(input).asInstanceOf[UnsafeArrayData].toByteArray)
    } else if (expression.isInstanceOf[PointWrapper]) {
      BindReferences.bindReference(expression, schema).eval(input).asInstanceOf[Shape]
    } else throw new UnsupportedOperationException("Query shape should be of ShapeType")
  }

  def getShape(exp: Expression, input: InternalRow): Shape = {
    if(!exp.isInstanceOf[PointWrapper] && exp.dataType.isInstanceOf[ShapeType]) {
      ShapeSerializer.deserialize(exp.eval(input).asInstanceOf[UnsafeArrayData].toByteArray())
    }else if(exp.isInstanceOf[PointWrapper]) {
      exp.eval(input).asInstanceOf[Shape]
    }else throw new UnsupportedOperationException("Query shape should be of ShapeType")
  }
}