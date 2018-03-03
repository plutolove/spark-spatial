package org.apache.spark.sql.spatial.index

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import scala.collection.mutable.ArrayBuffer

/*
 *   Created by plutolove on 02/03/2018.
 */
case class IndexData(name: String, plan: LogicalPlan, indexrelation: IndexRelation)

case class IndexInfo(tablename: String, indexname: String,
                     attrs: Seq[Attribute], indextype: IndexType,
                     storageLevel: StorageLevel) extends Serializable
class IndexManager extends Logging {
  @transient
  private val indexedData = new ArrayBuffer[IndexData]

  @transient
  private val indexLock = new ReentrantReadWriteLock

  @transient
  private val indexInfos = new ArrayBuffer[IndexInfo]

  def getIndexInfo: Array[IndexInfo] = indexInfos.toArray
  def getIndexData: Array[IndexData] = indexedData.toArray
  private def readLock[A](f: => A): A = {
    val lock = indexLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  private def writeLock[A](f: => A): A = {
    val lock = indexLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  def isEmpty: Boolean = readLock {
    indexedData.isEmpty
  }

  def lookupIndexedData(plan: LogicalPlan): Option[IndexData] = readLock {
    val tmp_res = indexedData.find{cd =>
      println(plan+"\n*\n"+cd.plan)
      plan.sameResult(cd.plan)}
    if (tmp_res.nonEmpty) return tmp_res
    else {
      indexedData.find(cd => {
        cd.plan match {
          case tmp_plan: SubqueryAlias =>
            plan.sameResult(tmp_plan.child)
          case _ => false
        }
      })
    }
  }

  def lookupIndexedData(query: Dataset[_], indexName: String): Option[IndexData] =
    readLock {
      lookupIndexedData(query.queryExecution.analyzed, indexName)
    }

  def lookupIndexedData(plan: LogicalPlan, indexName: String): Option[IndexData] =
    readLock {
      val tmp_res = indexedData.find(cd => plan.sameResult(cd.plan) && cd.name.equals(indexName))
      if (tmp_res.nonEmpty) return tmp_res
      else {
        indexedData.find(cd => {
          cd.plan match {
            case tmp_plan: SubqueryAlias =>
              plan.sameResult(tmp_plan.child) && cd.name.equals(indexName)
            case _ => false
          }
        })
      }
    }

  def createIndex(query: Dataset[_], indexType: IndexType, indexName: String,
                       column: List[Attribute], tableName: Option[String] = None,
                       storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = {
    writeLock {
      val planToIndex = query.queryExecution.analyzed

      println("plantoindex: "+planToIndex)

      if (lookupIndexedData(planToIndex).nonEmpty) {
        println("Index for the data has already been built.")
      } else {
        indexedData +=
          IndexData(indexName, planToIndex,
            IndexRelation(query.queryExecution.executedPlan, tableName,
              indexType, column, indexName))

        indexInfos += IndexInfo(tableName.getOrElse("Pluto"), indexName,
          column, indexType, storageLevel)

        println("create index")
      }
    }
  }

  def useIndexedData(plan: LogicalPlan): LogicalPlan = {
    //println("use index data")
    //println(plan)
    val tmp = plan transformDown {
      case currentFragment =>
        lookupIndexedData(currentFragment)
          .map(_.indexrelation.withOutput(currentFragment.output))
          .getOrElse(currentFragment)
    }
    tmp
  }
}
