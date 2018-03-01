package org.apache.spark.sql.spatial.index.partitioner

import java.awt.Dimension

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.spatial.shapes.{MBR, Point}
import org.apache.spark.sql.spatial.index.RTree
import org.apache.spark.util.{MutablePair, SizeEstimator}

import scala.collection.mutable

/*
 *   Created by plutolove on 01/03/2018.
 */

object STRPartition {
  def apply(data: RDD[(Point, InternalRow)], dimension: Int, partition: Int,
            sample_rate: Double, threshold: Long, max_entries_per_node: Int): (RDD[(Point, InternalRow)], Array[(MBR, Int)]) = {
    var rdd = data.mapPartitions{
      iter =>
        val mutpair = new MutablePair[Point, InternalRow]()
        iter.map(row => mutpair.update(row._1, row._2.copy()))
    }

    val partitioner = new STRPartitioner(partition, sample_rate, dimension, threshold, max_entries_per_node, rdd)

    val shuffledata = new ShuffledRDD[Point, InternalRow, InternalRow](rdd, partitioner)
    (shuffledata, partitioner.mbrs)
  }
}

class STRPartitioner(partition: Int, sample_rate: Double,
                     dimension: Int, threshold: Long,
                     max_entries_per_node: Int,
                     rdd: RDD[_ <: Product2[Point, Any]]) extends Partitioner {
  case class Bound(min: Array[Double], max: Array[Double])

  //get RDD max/min bound and the total size
  val (data_bounds, total_size) = {
    rdd.aggregate[(Bound, Long)]((null, 0))(
      (bound, data) => {
        val curr_bound = if(bound._1 == null) {
          Bound(data._1.coord, data._1.coord)
        } else {
          Bound(bound._1.min.zip(data._1.coord).map(x => Math.min(x._1, x._2)),
            bound._1.max.zip(data._1.coord).map(x => Math.max(x._1, x._2)))
        }
        (curr_bound, bound._2 + SizeEstimator.estimate(data._1))
      },(left, right) => {
        val curr_bound = if(left == null) {
          right._1
        }else if(right == null) {
          left._1
        }else {
          Bound(left._1.min.zip(right._1.min).map(x => Math.min(x._1, x._2)),
            left._1.max.zip(right._1.max).map(x => Math.min(x._1, x._2)))
        }
        (curr_bound, left._2 + right._2)
      }
    )
  }



  var (mbrs, partitions) = {
    val seed = System.currentTimeMillis()
    val sampledata = if(total_size < threshold * 0.2) {
      rdd.mapPartitions(iter => iter.map(_._1)).collect()
    } else if(total_size * sample_rate < threshold) {
      rdd.sample(withReplacement = false, sample_rate, seed).map(_._1).collect()
    } else {
      rdd.sample(withReplacement = false, threshold.toDouble / total_size, seed).map(_._1).collect()
    }


    val dim = new Array[Int](dimension)
    var rem = partition.toDouble
    for(i <- 0 until(dimension)) {
      dim(i) = Math.ceil(Math.pow(rem, 1.0 / (dimension - i))).toInt
      rem /= dim(i)
    }

    def recursiveGroupPoint(entries: Array[Point], min_item: Array[Double],
                            max_item: Array[Double], now_dim: Int, total_dim: Int): Array[MBR] = {
      val len = entries.length
      val grouped_data = entries.sortWith(_.coord(now_dim) < _.coord(now_dim))
        .grouped(Math.ceil(len * 1.0 / dim(now_dim)).toInt).toArray
      var ret = mutable.ArrayBuffer[MBR]()
      if(now_dim < total_dim) {
        for(i <- grouped_data.indices) {
          val curr_min = min_item
          val curr_max = max_item
          if(i == 0 && i == grouped_data.length - 1) {
            curr_min(now_dim) = data_bounds.min(now_dim)
            curr_max(now_dim) = data_bounds.max(now_dim)
          }else if(i == 0) {
            curr_min(now_dim) = data_bounds.min(now_dim)
            curr_max(now_dim) = grouped_data(i + 1).head.coord(now_dim)
          }else if(i == grouped_data.length - 1) {
            curr_min(now_dim) = grouped_data(i).head.coord(now_dim)
            curr_max(now_dim) = data_bounds.max(now_dim)
          }else {
            curr_min(now_dim) = grouped_data(i).head.coord(now_dim)
            curr_max(now_dim) = grouped_data(i + 1).head.coord(now_dim)
          }
          ret ++= recursiveGroupPoint(grouped_data(i), curr_min, curr_max, now_dim+1, total_dim)
        }
        ret.toArray
      }else {
        for(i <- grouped_data.indices) {
          if(i == 0 && i == grouped_data.length - 1) {
            min_item(now_dim) = data_bounds.min(now_dim)
            max_item(now_dim) = data_bounds.max(now_dim)
          }else if(i == 0) {
            min_item(now_dim) = data_bounds.min(now_dim)
            max_item(now_dim) = grouped_data(i + 1).head.coord(now_dim)
          }else if(i == grouped_data.length - 1) {
            min_item(now_dim) = grouped_data(i).head.coord(now_dim)
            max_item(now_dim) = data_bounds.max(now_dim)
          }else {
            min_item(now_dim) = grouped_data(i).head.coord(now_dim)
            max_item(now_dim) = grouped_data(i + 1).head.coord(now_dim)
          }
          ret += MBR(new Point(min_item), new Point(max_item))
        }
        ret.toArray
      }
    }

    val cur_min = new Array[Double](dimension)
    val cur_max = new Array[Double](dimension)
    val mbrs = recursiveGroupPoint(sampledata, cur_min, cur_max, 0, dimension - 1)
    (mbrs.zipWithIndex, mbrs.length)
  }

  val rtree = RTree(mbrs.map(x => (x._1, x._2, 1)), max_entries_per_node)

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val point = key.asInstanceOf[Point]
    rtree.circleRange(point, 0).head._2
  }
}
