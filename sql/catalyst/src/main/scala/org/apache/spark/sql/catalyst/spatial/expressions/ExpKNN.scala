package org.apache.spark.sql.catalyst.spatial.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.catalyst.spatial.util.NumberUtil
/*
 *   Created by plutolove on 08/02/2018.
 */
case class ExpKNN(shape: Expression, target: Expression, k: Literal) extends Predicate with CodegenFallback {
  require(NumberUtil.isInteger(k.dataType))
  override def children: Seq[Expression] = Seq(shape, target, k)

  override def nullable: Boolean = false

  override def toString: String = s" **($shape) IN KNN ($target) within ($k)"

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = true
}