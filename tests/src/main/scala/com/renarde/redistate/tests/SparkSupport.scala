package com.renarde.redistate.tests

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSupport extends BeforeAndAfterAll {
    self: Suite =>

    var spark: SparkSession = _

    override def beforeAll(): Unit = {
        spark = SparkSession.builder()
            .appName("redistate-spark-test")
            .master("local[*]")
            .config("spark.sql.streaming.stateStore.providerClass", "com.renarde.redistate.store.RedisStateStoreProvider")
            .config("spark.sql.streaming.stateStore.redis.host", "localhost")
            .config("spark.sql.streaming.stateStore.redis.port", "6379")
            .config("spark.sql.streaming.stateStore.redis.prefix", "redistate")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()

        super.beforeAll()
    }

    override def afterAll(): Unit = {
        spark.stop()
        super.afterAll()
    }
}
