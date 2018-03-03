package org.apache.spark.sql.spatial.execution

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.spatial.shapes.{MBR, Point, Shape}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.spatial.util.ShapeUtil
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.spatial.expressions.{ExpKNN, ExpRange}
import org.apache.spark.sql.spatial.index.{IndexUtil, RTree, RTreeIndexRelation}

/*
 *   Created by plutolove on 08/02/2018.
 */
case class SpatialFilterExec(condition: Expression, child: SparkPlan, lp: LogicalPlan) extends SparkPlan with PredicateHelper {
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

  def evalDist(row: InternalRow, origin: Point, attrs: Seq[Attribute], plan: LogicalPlan): Double = {
    origin.minDist(IndexUtil.getPointFromRow(row, attrs, plan))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val root_rdd = child.execute()
    condition match {
      case ExpKNN(point, target, k) =>
        val _target = target.asInstanceOf[Literal].value.asInstanceOf[Point]
        val _k = k.value.asInstanceOf[Number].intValue()
        var flag: Boolean = true
        val session = SparkSession.getActiveSession.orNull
        if(session == null) {
          flag = false
        }
        var indexdata = session.sessionState.indexManager.lookupIndexedData(lp.children.head).orNull
        //session.sessionState.indexManager.getIndexInfo.foreach(println)
        //session.sessionState.indexManager.getIndexData.foreach(row => println(row.plan))

        if(indexdata == null) {
          //println("***************")
          flag = false
        }

        if(flag) {
          //println("with index")
          val rtree = indexdata.indexrelation.asInstanceOf[RTreeIndexRelation]
          val col_keys = rtree.col_keys
          val global_part1 = rtree.global_rtree.kNN(_target, _k, keepSame = false).map(_._2).toSet
          val ord = new DistanceOrdering(point, _target)

          def knnGlobalPrune(global_part: Set[Int]): Array[InternalRow] = {
            val pruned = new PartitionPruningRDD(rtree.indexRDD_data, global_part.contains)
            pruned.flatMap{ packed =>
              var tmp_ans = Array[(Shape, Int)]()
              if (packed.index.asInstanceOf[RTree] != null) {
                tmp_ans = packed.index.asInstanceOf[RTree].kNN(_target, _k, keepSame = false)
              }
              tmp_ans.map(x => packed.data(x._2))
            }.takeOrdered(_k)(ord)
          }

          val tmp_ans = knnGlobalPrune(global_part1)
          val theta = evalDist(tmp_ans.last, _target, col_keys, rtree)
          val global_part2 = rtree.global_rtree.circleRange(_target, theta).map(_._2).toSet -- global_part1
          val tmp_knn_res = if (global_part2.isEmpty) tmp_ans
          else knnGlobalPrune(global_part2).union(tmp_ans).sorted(ord).take(_k)
          sparkContext.parallelize(tmp_knn_res)
        } else {
          knn(root_rdd, point, _target, _k)
        }
      case ExpRange(shape, low, high) =>
        var flag: Boolean = true
        //val data = ShapeUtil.getShape(shape, input)
        val low_point = low.asInstanceOf[Literal].value.asInstanceOf[Point]
        val high_point = high.asInstanceOf[Literal].value.asInstanceOf[Point]
        //require(data.dimensions == low_point.dimensions && low_point.dimensions == high_point.dimensions)
        val query_mbr = MBR(low_point, high_point)

        val session = SparkSession.getActiveSession.orNull
        if(session == null) {
          flag = false
        }
        var indexdata = session.sessionState.indexManager.lookupIndexedData(lp.children.head).orNull

        if(indexdata == null) {
          flag = false
        }
        if(flag){
          println("-------------------------range index-------------")
          val rtree = indexdata.indexrelation.asInstanceOf[RTreeIndexRelation]
          val col_keys = rtree.col_keys

          var global_part = rtree.global_rtree.range(query_mbr).map(_._2).toSeq

          global_part.foreach(println)

          val prdd = new PartitionPruningRDD(rtree.indexRDD_data, global_part.contains)
          val tmp = prdd.flatMap{datas =>
            val index = datas.index.asInstanceOf[RTree]
            if(index != null) {
              val root_mbr = index.root.m_mbr
              val perfect_cover = query_mbr.contains(root_mbr.low) &&
                query_mbr.contains(root_mbr.high)

              if(perfect_cover) {
                datas.data
              } else {
                index.range(query_mbr).map(x => datas.data(x._2))
              }
            }else Array[InternalRow]()
          }
          tmp
        } else {
          root_rdd.mapPartitions(iter => iter.filter(newPredicate(condition, child.output).eval(_)))
        }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def children: Seq[SparkPlan] = child :: Nil
  override def outputPartitioning: Partitioning = child.outputPartitioning
}