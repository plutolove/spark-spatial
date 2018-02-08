package org.apache.spark.sql.spatial.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.spatial.shapes.Point
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.spatial.util.ShapeUtil
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.spatial.expressions.{ExpKNN, ExpRange}

/*
 *   Created by plutolove on 08/02/2018.
 */
case class SpatialFilterExec(condition: Expression, child: SparkPlan) extends SparkPlan with PredicateHelper {
  override def output: Seq[Attribute] = child.output

  private class DistanceOrdering(point: Expression, target: Point) extends Ordering[InternalRow] {
    override def compare(x: InternalRow, y: InternalRow): Int = {
      val px = ShapeUtil.getShape(point, child.output, x)
      val py = ShapeUtil.getShape(point, child.output, y)
      val dis_x = target.minDist(px)
      val dis_y = target.minDist(py)
      dis_x.compare(dis_y)
    }
  }

  def knn(rdd: RDD[InternalRow], point: Expression, target: Point, k: Int): RDD[InternalRow] = {
    sparkContext.parallelize(rdd.map(_.copy()).takeOrdered(k)(new DistanceOrdering(point, target)), 1)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val root_rdd = child.execute()
    condition match {
      case ExpKNN(point, target, k) =>
        val _target = target.asInstanceOf[Literal].value.asInstanceOf[Point]
        val _k = k.value.asInstanceOf[Number].intValue()
        knn(root_rdd, point, _target, _k)
      //case _ =>
        //root_rdd.mapPartitions(iter => iter.filter(newPredicate(condition, child.output)))
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def children: Seq[SparkPlan] = child :: Nil
  override def outputPartitioning: Partitioning = child.outputPartitioning
}