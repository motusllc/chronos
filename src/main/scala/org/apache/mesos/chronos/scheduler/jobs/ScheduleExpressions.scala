package org.apache.mesos.chronos.scheduler.jobs

import java.util.TimeZone
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Period
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.format.ISOPeriodFormat
import com.github.buster84.cron.Schedule
import org.joda.time.Seconds
import java.util.logging.Logger
import org.joda.time.format.DateTimeFormat
import com.github.buster84.cron.Schedule

/**
 * Parsing, creating and validation for schedule expressions, either Iso8601 or cron.
 * @author Florian Leibert (flo@leibert.de)
 */
object ScheduleExpressions {
  val iso8601ExpressionRegex = """(R[0-9]*)/(.*)/(P.*)""".r
  private[this] val formatter = ISODateTimeFormat.dateTime
  private[this] val log = Logger.getLogger(getClass.getName)
  
  /**
   * Verifies that the given expression is a valid Iso8601Expression. Currently not all Iso8601Expression formats
   * are supported.
   * @param input
   * @return
   */
  def canParse(input: String, timeZoneStr: String = ""): Boolean = {
    parse(input, timeZoneStr) match {
      case Some(_) =>
        true
      case None =>
        false
     }
  }

  def parse(input: String, timeZoneStr: String = ""): Option[JobSchedule] = {
    // Try parsing ISO 8601 first
    parseIso8601(input, timeZoneStr)  match {
      case Some(sch) =>
        Some(sch)
      case None =>
        // Otherwise try with cron
        parseCron(input, timeZoneStr)  match {
          case Some(sch) =>
            Some(sch)
          case None =>
            None
        }
     }
  }
  
  /**
   * Parses a ISO8601 expression into a tuple consisting of the number of repetitions (or -1 for infinity),
   * the start
   * @param input the input string which is a ISO8601 expression consisting of Repetition, Start and Period.
   * @return a two tuple (repetitions, start,)
   */
  def parseIso8601(input: String, timeZoneStr: String = ""): Option[JobSchedule] = {
    try {

      val iso8601ExpressionRegex(repeatStr, startStr, periodStr) = input

      val repeat: Long = {
        if (repeatStr.length == 1)
          -1L
        else
          repeatStr.substring(1).toLong
      }

      val start: DateTime = if (startStr.length == 0) DateTime.now(DateTimeZone.UTC) else convertToDateTime(startStr, timeZoneStr)
      val period: Period = ISOPeriodFormat.standard.parsePeriod(periodStr)
      Some(JobSchedule(input, timeZoneStr, start, start, repeat, period))
    } catch {
      case e: scala.MatchError =>
        None
      case e: IllegalArgumentException =>
        None
    }
  }
  
  /**
   * Parses a cron expression into a tuple consisting of the number of repetitions and the start
   */
  def parseCron(input: String, timeZoneStr: String = ""): Option[JobSchedule] = {
    try {
     val schedule = Schedule(input, DateTimeZone.forID(timeZoneStr))
     
     // Cron expressions repeat indefinitely
     val repeat = -1L
     
     // Get the start time
     val nextExecution = schedule.getNextAfter(DateTime.now)
     
     Some(JobSchedule(input, timeZoneStr, nextExecution, nextExecution, repeat, null))
    } catch {
      case e: IllegalArgumentException =>
        None
    }
  }

  /**
   * Creates a DateTime object from an input string.  This parses the object by first checking for a time zone and then
   * using a datetime formatter to format the date and time.
   * @param dateTimeStr the input date time string with optional time zone
   * @return the date time
   */
  def convertToDateTime(dateTimeStr: String, timeZoneStr: String): DateTime = {
    val dateTime = DateTime.parse(dateTimeStr)
    if (timeZoneStr != null && timeZoneStr.length > 0) {
      val timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneStr))
      dateTime.withZoneRetainFields(timeZone)
    } else {
      dateTime
    }
  }

  /**
   * Creates a valid Iso8601Expression based on the input parameters.
   * @param recurrences
   * @param startDate
   * @param period
   * @return
   */
  def createIso8601String(recurrences: Long, startDate: DateTime, period: Period): String = {
    if (recurrences != -1)
      "R%d/%s/%s".format(recurrences, formatter.print(startDate), ISOPeriodFormat.standard.print(period))
    else
      "R/%s/%s".format(formatter.print(startDate), ISOPeriodFormat.standard.print(period))
  }
  
  /**
   * Returns a schedule with the next time that this job should run
   */
  def next(schedule: JobSchedule, dateTime: DateTime): Option[JobSchedule] = {
    // If the schedule has a period, then skip forward until the future
    if (schedule.period != null) {
      val skip = calculateSkips(dateTime, schedule.nextRun, schedule.period)
      if (schedule.recurrences == -1) {
        val nStart = schedule.nextRun.plus(schedule.period.multipliedBy(skip))
        log.warning("Skipped forward %d iterations, modified start from '%s' to '%s"
          .format(skip, schedule.nextRun.toString(DateTimeFormat.fullDate),
            nStart.toString(DateTimeFormat.fullDate)))
        Some(new JobSchedule(createIso8601String(schedule.recurrences, nStart, schedule.period), 
              schedule.scheduleTimeZone,
              schedule.firstRun,
              nStart,
              schedule.recurrences,
              schedule.period))
      } else if (schedule.recurrences < skip) {
        log.warning("Filtered job as it is no longer valid.")
        None
      } else {
        val nRec = schedule.recurrences - skip
        val nStart = schedule.nextRun.plus(schedule.period.multipliedBy(skip))
        log.warning("Skipped forward %d iterations, iterations is now '%d' , modified start from '%s' to '%s"
          .format(skip, nRec, schedule.nextRun.toString(DateTimeFormat.fullDate),
            nStart.toString(DateTimeFormat.fullDate)))
        Some(new JobSchedule(createIso8601String(nRec, nStart, schedule.period), 
              schedule.scheduleTimeZone,
              schedule.firstRun,
              nStart,
              nRec,
              schedule.period))
      }
    } else {
      // Cron schedule
      val cronSchedule = Schedule(schedule.scheduleString, DateTimeZone.forID(schedule.scheduleTimeZone))
      Some(new JobSchedule(schedule.scheduleString, 
              schedule.scheduleTimeZone,
              schedule.firstRun,
              cronSchedule.getNextAfter(dateTime),
              schedule.recurrences,
              schedule.period))
    }
  }
  
    /**
   * Calculates the number of skips needed to bring the job start into the future
   */
  protected def calculateSkips(dateTime: DateTime, jobStart: DateTime, period: Period): Int = {
    // If the period is at least a month, we have to actually add the period to the date
    // until it's in the future because a month-long period might have different seconds
    if (period.getMonths >= 1) {
      var skips = 0
      var newDate = new DateTime(jobStart)
      while (newDate.isBefore(dateTime)) {
        newDate = newDate.plus(period)
        skips += 1
      }
      skips
    } else {
      Seconds.secondsBetween(jobStart, dateTime).getSeconds / period.toStandardSeconds.getSeconds
    }
  }
}
