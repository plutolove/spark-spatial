package org.apache.spark.sql.spatial.index

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

/*
 *   Created by plutolove on 02/03/2018.
 */

object IndexRelation {
  def apply(sp: SparkPlan, table_name: Option[String], index_type: IndexType,
            attrs: List[Attribute], index_name: String): IndexRelation = {
    index_type match {

      case _ => null
    }
  }
}

abstract class IndexRelation extends LogicalPlan {
  self: Product =>
  var indexRDD_data: IndexRDD
  def indexRDD = indexRDD_data

  def sparksession = SparkSession.getActiveSession.orNull

  override def children: Seq[LogicalPlan] = Nil

  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexRelation
}
