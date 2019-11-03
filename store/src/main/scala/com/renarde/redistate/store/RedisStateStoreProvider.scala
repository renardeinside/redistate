package com.renarde.redistate.store

import com.renarde.redistate.store.RedisStateStoreProvider._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreMetrics, StateStoreProvider, UnsafeRowPair}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

/**
 * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
 * by files in a HDFS-compatible file system using Redis key-value storage format.
 * All updates to the store has to be done in sets transactionally, and each set of updates
 * increments the store's version. These versions can be used to re-execute the updates
 * (by retries in RDD operations) on the correct version of the store, and regenerate
 * the store version.
 *
 * Usage:
 * To update the data in the state store, the following order of operations are needed.
 *
 * // get the right store
 * - val store = StateStore.get(
 * StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)
 * - store.put(...)
 * - store.remove(...)
 * - store.commit()    // commits all the updates to made; the new version will be returned
 * - store.iterator()  // key-value data after last commit as an iterator
 * - store.updates()   // updates made in the last commit as an iterator
 *
 * Fault-tolerance model:
 * TODO: add
 *
 */


class RedisStateStoreProvider extends StateStoreProvider with Logging {

  type MapType = java.util.concurrent.ConcurrentHashMap[UnsafeRow, UnsafeRow]

  @volatile private var redisConf: RedisConf = _
  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  private lazy val baseDir = stateStoreId.storeCheckpointLocation()
  private lazy val fm = CheckpointFileManager.create(baseDir, hadoopConf)
  override def init(
                     stateStoreId: StateStoreId,
                     keySchema: StructType,
                     valueSchema: StructType,
                     keyIndexOrdinal: Option[Int],
                     storeConf: StateStoreConf,
                     hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.redisConf = setRedisConf(this.storeConf.confs)
    log.info("Initializing the redis state store")
    fm.mkdirs(baseDir)

  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override def close(): Unit = ???

  override def getStore(version: Long): StateStore = {
    require(version >= 0, "Version cannot be less than 0")
    val newMap = new MapType()
    val store = new RedisStateStore(version, newMap)
    logInfo(s"Retrieved version $version of ${RedisStateStoreProvider.this} for update")
    store
  }

  override def toString: String = {
    s"RedisStateStoreProvider[id = (op=${stateStoreId.operatorId},part=${stateStoreId.partitionId}),redis=$redisConf]"
  }

  class RedisStateStore(val version: Long, mapToUpdate: MapType) extends StateStore {
    override def id: StateStoreId = ???

    override def get(key: UnsafeRow): UnsafeRow = ???

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = ???

    override def remove(key: UnsafeRow): Unit = ???

    override def commit(): Long = ???

    override def abort(): Unit = ???

    override def iterator(): Iterator[UnsafeRowPair] = ???

    override def metrics: StateStoreMetrics = ???

    override def hasCommitted: Boolean = ???
  }
}


object RedisStateStoreProvider {
  final val DEFAULT_REDIS_HOST: String = "localhost"
  final val DEFAULT_REDIS_PORT: String = "6379"
  final val REDIS_HOST: String = "spark.sql.streaming.stateStore.redis.host"
  final val REDIS_PORT: String = "spark.sql.streaming.stateStore.redis.port"

  private def setRedisConf(conf: Map[String, String]): RedisConf = {
    val host = Try(conf.getOrElse(REDIS_HOST, DEFAULT_REDIS_HOST)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    val port = Try(conf.getOrElse(REDIS_PORT, DEFAULT_REDIS_PORT).toInt) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
    RedisConf(host, port)
  }

}