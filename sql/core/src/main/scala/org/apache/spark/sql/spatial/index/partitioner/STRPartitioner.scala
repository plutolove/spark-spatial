package org.apache.spark.sql.spatial.index.partitioner

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
  def apply(origin: RDD[(Point, InternalRow)], dimension: Int, est_partition: Int,
            sample_rate: Double, transfer_threshold: Long, max_entries_per_node: Int)
  : (RDD[(Point, InternalRow)], Array[(MBR, Int)]) = {

    val rdd = origin.mapPartitions {iter =>
        val mutablePair = new MutablePair[Point, InternalRow]()
        iter.map(row => mutablePair.update(row._1, row._2.copy()))
      }

    println("STRPartition: "+est_partition)
    val part = new STRPartitioner(est_partition, sample_rate, dimension,
      transfer_threshold, max_entries_per_node, rdd)
    val shuffled = new ShuffledRDD[Point, InternalRow, InternalRow](rdd, part)
    (shuffled, part.mbrBound)
  }
}

class STRPartitioner(est_partition: Int,
                     sample_rate: Double,
                     dimension: Int,
                     transfer_threshold: Long,
                     max_entries_per_node: Int,
                     rdd: RDD[_ <: Product2[Point, Any]])
  extends Partitioner {
  def numPartitions: Int = partitions

  private case class Bounds(min: Array[Double], max: Array[Double])

  var (mbrBound, partitions) = {
    val (data_bounds, total_size) = {
      rdd.aggregate[(Bounds, Long)]((null, 0))((bound, data) => {
        val new_bound = if (bound._1 == null) {
          Bounds(data._1.coord, data._1.coord)
        } else {
          Bounds(bound._1.min.zip(data._1.coord).map(x => Math.min(x._1, x._2)),
            bound._1.max.zip(data._1.coord).map(x => Math.max(x._1, x._2)))
        }
        (new_bound, bound._2 + SizeEstimator.estimate(data._1))
      }, (left, right) => {
        val new_bound = {
          if (left._1 == null) right._1
          else if (right._1 == null) left._1
          else {
            Bounds(left._1.min.zip(right._1.min).map(x => Math.min(x._1, x._2)),
              left._1.max.zip(right._1.max).map(x => Math.max(x._1, x._2)))
          }}
        (new_bound, left._2 + right._2)
      })
    }

    data_bounds.min.foreach(item => print(s"${item} , "))
    //println("")
    data_bounds.max.foreach(item => print(s"${item}, "))
    //println("")

    val seed = System.currentTimeMillis()
    val sampled =  if (total_size * sample_rate <= 0.05 * transfer_threshold) {
      rdd.mapPartitions(part => part.map(_._1)).collect()
    } else if (total_size * sample_rate <= transfer_threshold) {
      rdd.sample(withReplacement = false, sample_rate, seed).map(_._1).collect()
    } else {
      rdd.sample(withReplacement = false, transfer_threshold.toDouble / total_size, seed)
        .map(_._1).collect()
    }

    val dim = new Array[Int](dimension)
    var remaining = est_partition.toDouble
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
      println("*****: "+dim(i))
    }

    def recursiveGroupPoint(entries: Array[Point], now_min: Array[Double],
                            now_max: Array[Double], cur_dim: Int, until_dim: Int): Array[MBR] = {
      val len = entries.length
      val grouped = entries.sortWith(_.coord(cur_dim) < _.coord(cur_dim))
        .grouped(Math.ceil(len * 1.0 / dim(cur_dim)).toInt).toArray
      var ans = mutable.ArrayBuffer[MBR]()
      if (cur_dim < until_dim) {
        for (i <- grouped.indices) {
          val cur_min = now_min
          val cur_max = now_max
          if (i == 0 && i == grouped.length - 1) {
            cur_min(cur_dim) = data_bounds.min(cur_dim)
            cur_max(cur_dim) = data_bounds.max(cur_dim)
          } else if (i == 0) {
            cur_min(cur_dim) = data_bounds.min(cur_dim)
            cur_max(cur_dim) = grouped(i + 1).head.coord(cur_dim)
          } else if (i == grouped.length - 1) {
            cur_min(cur_dim) = grouped(i).head.coord(cur_dim)
            cur_max(cur_dim) = data_bounds.max(cur_dim)
          } else {
            cur_min(cur_dim) = grouped(i).head.coord(cur_dim)
            cur_max(cur_dim) = grouped(i + 1).head.coord(cur_dim)
          }
          ans ++= recursiveGroupPoint(grouped(i), cur_min, cur_max, cur_dim + 1, until_dim)
        }
        ans.toArray
      } else {
        for (i <- grouped.indices) {
          if (i == 0 && i == grouped.length - 1) {
            now_min(cur_dim) = data_bounds.min(cur_dim)
            now_max(cur_dim) = data_bounds.max(cur_dim)
          } else if (i == 0) {
            now_min(cur_dim) = data_bounds.min(cur_dim)
            now_max(cur_dim) = grouped(i + 1).head.coord(cur_dim)
          } else if (i == grouped.length - 1) {
            now_min(cur_dim) = grouped(i).head.coord(cur_dim)
            now_max(cur_dim) = data_bounds.max(cur_dim)
          } else {
            now_min(cur_dim) = grouped(i).head.coord(cur_dim)
            now_max(cur_dim) = grouped(i + 1).head.coord(cur_dim)
          }
          ans += MBR(new Point(now_min.clone()), new Point(now_max.clone()))
        }
        ans.toArray
      }
    }

    val cur_min = new Array[Double](dimension)
    val cur_max = new Array[Double](dimension)
    val mbrs = recursiveGroupPoint(sampled, cur_min, cur_max, 0, dimension - 1)

    (mbrs.zipWithIndex, mbrs.length)
  }
  //println(est_partition+"----------------"+numPartitions+"--------------"+dimension)
  val rt = RTree(mbrBound.map(x => (x._1, x._2, 1)), max_entries_per_node)

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Point]
    rt.circleRange(k, 0.0).head._2
  }
}