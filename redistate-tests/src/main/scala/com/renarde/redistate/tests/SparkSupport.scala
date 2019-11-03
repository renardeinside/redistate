package com.renarde.redistate.tests

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSupport extends BeforeAndAfterAll {
  self: Suite =>

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("redistore-spark-test")
      .master("local[*]")
      .getOrCreate()

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
