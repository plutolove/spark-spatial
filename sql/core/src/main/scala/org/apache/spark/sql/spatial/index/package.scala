package org.apache.spark.sql.spatial

import org.apache.spark.rdd.RDD
/*
 *   Created by plutolove on 02/03/2018.
 */

package object index {
  type IndexRDD = RDD[IndexPartition]
}
