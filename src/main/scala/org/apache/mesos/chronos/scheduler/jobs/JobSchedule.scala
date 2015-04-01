package org.apache.mesos.chronos.scheduler.jobs

import org.joda.time.DateTime
import org.joda.time.Period

case class JobSchedule(
  val scheduleString: String,
  val scheduleTimeZone: String,
  val firstRun: DateTime,
  val nextRun: DateTime,
  val recurrences: Long,
  val period: Period) {
  
}