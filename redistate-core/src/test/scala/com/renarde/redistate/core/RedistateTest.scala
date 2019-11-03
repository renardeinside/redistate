package com.renarde.redistate.core

import com.renarde.redistate.tests.{RedisSupport, SparkSupport}
import org.scalatest.FunSuite
import redis.embedded.RedisServer

class RedistateTest extends FunSuite with SparkSupport with RedisSupport {

  test("simple redis with spark mock test") {
    assert(spark.sparkContext.isStopped)
    assert(redisServer.isInstanceOf[RedisServer])
  }
}
