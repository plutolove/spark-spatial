package org.apache.spark.sql.catalyst.spatial.util

import org.apache.spark.sql.types._

/*
 *   Created by plutolove on 08/02/2018.
 */
object NumberUtil {
  def isInteger(x: DataType): Boolean = {
    x match {
      case IntegerType => true
      case LongType => true
      case ShortType => true
      case ByteType => true
      case _ => false
    }
  }
}