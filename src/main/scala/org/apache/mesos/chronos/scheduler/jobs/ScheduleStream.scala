package org.apache.mesos.chronos.scheduler.jobs

import org.joda.time.DateTime

/**
 * A stream of schedules.
 * Calling tail will return a clipped schedule.
 * The schedule consists of a string representation of an ISO8601 expression as well as a BaseJob.
 * @author Florian Leibert (flo@leibert.de)
 */
class ScheduleStream(val schedule: JobSchedule, val jobName: String) {

  def head(): (JobSchedule, String) = {
    (schedule, jobName)
  }

  /**
   * Returns a clipped schedule.
   * @return
   */
  def tail(): Option[ScheduleStream] = {
    ScheduleExpressions.next(schedule, DateTime.now) match {
      case Some(sch) =>
          Some(new ScheduleStream(sch, jobName))
      case None =>
        None
    }
  }
}
