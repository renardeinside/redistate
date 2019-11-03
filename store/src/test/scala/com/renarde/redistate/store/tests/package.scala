package com.renarde.redistate.store

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.{Encoder, Encoders}

package object tests {
  val random = new scala.util.Random

  case class PageVisit(id: Int, url: String, timestamp: Timestamp = Timestamp.from(Instant.now()))

  case class UserStatistics(userId: Int, visits: Seq[PageVisit], totalVisits: Int)

  case class UserGroupState(groupState: UserStatistics)

  implicit val pageVisitEncoder: Encoder[PageVisit] = Encoders.product[PageVisit]
  implicit val userStatisticsEncoder: Encoder[UserStatistics] = Encoders.product[UserStatistics]
}
