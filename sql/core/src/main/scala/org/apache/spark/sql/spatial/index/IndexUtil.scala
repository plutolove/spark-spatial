package org.apache.spark.sql.spatial.index

import org.apache.spark.sql.catalyst.spatial.shapes.Point
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/*
 *   Created by plutolove on 03/03/2018.
 */
object IndexUtil {
  def getPointFromRow(row: InternalRow, cols: Seq[Attribute], sp: SparkPlan): Point = {
    Point(cols.toArray.map(BindReferences.bindReference(_, sp.output).eval(row).asInstanceOf[Number].doubleValue()))
  }

  def getPointFromRow(row: InternalRow, cols: Seq[Attribute], sp: LogicalPlan): Point = {
    Point(cols.toArray.map(BindReferences.bindReference(_, sp.output).eval(row).asInstanceOf[Number].doubleValue()))
  }
}
