package org.apache.spark.sql.spatial.index

import org.apache.spark.sql.catalyst.InternalRow

/*
 *   Created by plutolove on 28/02/2018.
 */
trait Index

case class IndexPartition(data: Array[InternalRow], index: Index)

object IndexType {
  def apply(str: String): IndexType = {
    str.toLowerCase match {
      case "rtree" => RTreeType
      case _ => null
    }
  }
}

abstract class IndexType

case object RTreeType extends IndexType