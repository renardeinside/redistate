package com.renarde.redistate

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.types.StructType

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
 * - Every set of updates is written to a snapshot file before committing.
 * - The state store is responsible for cleaning up of old snapshot files.
 * - Multiple attempts to commit the same version of updates may overwrite each other.
 * Consistency guarantees depend on whether multiple attempts have the same updates and
 * the overwrite semantics of underlying file system.
 * - Background maintenance of files ensures that last versions of the store is always
 * recoverable to ensure re-executed RDD operations re-apply updates on the correct
 * past version of the store.
 */
class RedisStateStoreProvider extends StateStoreProvider with Logging {

  override def init(stateStoreId: StateStoreId, keySchema: StructType, valueSchema: StructType, keyIndexOrdinal: Option[Int], storeConfs: StateStoreConf, hadoopConf: Configuration): Unit = ???

  override def stateStoreId: StateStoreId = ???

  override def close(): Unit = ???

  override def getStore(version: Long): StateStore = ???
}
