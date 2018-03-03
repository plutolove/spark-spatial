package org.apache.spark.sql.spatial.index

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.spatial.index.partitioner.STRPartition
import org.apache.spark.sql.types.NumericType
import org.apache.spark.storage.StorageLevel

/*
 *   Created by plutolove on 02/03/2018.
 */
case class RTreeIndexRelation(output: Seq[Attribute], child: SparkPlan, table_name: Option[String],
                              col_keys: Seq[Attribute], index_name: String)(var indexRDD_data: IndexRDD = null, var global_rtree: RTree = null) extends IndexRelation {

  //def column_keys: Seq[Attribute] = col_keys

  private def check_keys: Boolean = {
    if(col_keys.length > 0) {
      for(i <- col_keys.indices) {
        if(!col_keys(i).dataType.isInstanceOf[NumericType]) {
          return false
        }
      }
      true
    } else false
  }

  require(check_keys)

  val dimension = IndexUtil.getPointFromRow(child.execute().first(), col_keys, child).coord.length

  if(indexRDD_data == null) {
    buildIndex()
  }

  def buildIndex(): Unit = {
    val numPartitions = sparksession.sessionState.conf.indexPsrtitions
    val maxEntriesPerNode = sparksession.sessionState.conf.maxEntriesPerNode
    val sampleRate = sparksession.sessionState.conf.sampleRate
    val transferThreshold = sparksession.sessionState.conf.transfer_Threshold

    val tmpRDD = child.execute().map(row =>{
      (IndexUtil.getPointFromRow(row, col_keys, child), row)
    })

    val (partitionRDD, mbrs) = STRPartition(tmpRDD, dimension, numPartitions, sampleRate, transferThreshold, maxEntriesPerNode)
    //create local RTree index for each partition
    val indexedrdd = partitionRDD.mapPartitions{iter =>
      val tmpdata = iter.toArray
      var index: RTree = null
      if(tmpdata.length > 0) index = RTree(tmpdata.map(_._1).zipWithIndex, maxEntriesPerNode)
      Array(IndexPartition(tmpdata.map(_._2), index)).iterator
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitionSize = indexedrdd.mapPartitions(iter => iter.map(_.data.length)).collect()

    //create global RTree index
    global_rtree = RTree(mbrs.zip(partitionSize)
      .map(x => (x._1._1, x._1._2, x._2)), maxEntriesPerNode)
    indexedrdd.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    indexRDD_data = indexedrdd
  }

  override def withOutput(new_output: Seq[Attribute]): IndexRelation = {
    println("---------&&&&&&&&&")
    RTreeIndexRelation(new_output, child, table_name,
      col_keys, index_name)(indexRDD_data, global_rtree)
  }
}
