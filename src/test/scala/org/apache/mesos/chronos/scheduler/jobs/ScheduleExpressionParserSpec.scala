package org.apache.mesos.chronos.scheduler.jobs

import org.joda.time.{DateTime, Days, Period}
import org.specs2.mutable._

class ScheduleExpressionParserSpec extends SpecificationWithJUnit {
  "ScheduleExpressions$" should {
    "reject non-iso8601 expressions" in {
      ScheduleExpressions.parse("FOO") should be(None)
    }

    "properly parse expression" in {
      val fail = ScheduleExpressions.parse("R5/2008-03-01T13:00:00Z/P1D") match {
        case Some(sch:JobSchedule) =>
          sch.recurrences must_== 5L
          sch.firstRun must_== DateTime.parse("2008-03-01T13:00:00Z")
          //This is a hack because Period's equals seems broken!
          sch.period.toString must_== new Period(Days.ONE).toString
          false
        case _ =>
          true
      }
      fail must beEqualTo(false)
    }

    "properly parse infinite repetition" in {
      val fail = ScheduleExpressions.parse("R/2008-03-01T13:00:00Z/P1D") match {
        case Some(sch:JobSchedule) =>
          sch.recurrences must_== -1L
          sch.firstRun must_== DateTime.parse("2008-03-01T13:00:00Z")
          //This is a hack because Period's equals seems broken!
          sch.period.toString must_== new Period(Days.ONE).toString
          false
        case _ =>
          true
      }
      fail must beEqualTo(false)
    }

    "properly parse time zone (BST)" in {
      val fail = ScheduleExpressions.parse("R/2008-03-01T13:00:00Z/P1D", "BST") match {
        case Some(sch:JobSchedule) =>
          sch.recurrences must_== -1L
          sch.firstRun.getMillis must_== DateTime.parse("2008-03-01T13:00:00+06:00").getMillis
          //This is a hack because Period's equals seems broken!
          sch.period.toString must_== new Period(Days.ONE).toString
          false
        case _ =>
          true
      }
      fail must beEqualTo(false)
    }

    "properly parse time zone (PST)" in {
      val fail = ScheduleExpressions.parse("R/2008-03-01T13:00:00Z/P1D", "PST") match {
        case Some(sch:JobSchedule) =>
          sch.recurrences must_== -1L
          sch.firstRun.getMillis must_== DateTime.parse("2008-03-01T13:00:00-08:00").getMillis
          //This is a hack because Period's equals seems broken!
          sch.period.toString must_== new Period(Days.ONE).toString
          false
        case _ =>
          true
      }
      fail must beEqualTo(false)
    }

    "properly parse time zone (Europe/Paris)" in {
      val fail = ScheduleExpressions.parse("R/2008-03-01T13:00:00Z/P1D", "Europe/Paris") match {
        case Some(sch:JobSchedule) =>
          sch.recurrences must_== -1L
          sch.firstRun.getMillis must_== DateTime.parse("2008-03-01T13:00:00+01:00").getMillis
          //This is a hack because Period's equals seems broken!
          sch.period.toString must_== new Period(Days.ONE).toString
          false
        case _ =>
          true
      }
      fail must beEqualTo(false)
    }


    "Test precedence using two time zone options at once (EDT and PST)" in {
      val fail = ScheduleExpressions.parse("R/2008-03-01T13:00:00-04:00/P1D", "PST") match {
        case Some(sch:JobSchedule) =>
          sch.recurrences must_== -1L
          sch.firstRun.getMillis must_== DateTime.parse("2008-03-01T13:00:00-08:00").getMillis
          //This is a hack because Period's equals seems broken!
          sch.period.toString must_== new Period(Days.ONE).toString
          false
        case _ =>
          true
      }
      fail must beEqualTo(false)
    }

    "Test parse error when sch.period is not specified" in {
      val fail = ScheduleExpressions.parse("R/2008-03-01T13:00:00Z", "PST") match {
        case None =>
          false
        case _ =>
          true
      }
      fail must beEqualTo(false)
    }

    "Test time zone change when time is not specified" in {
      val fail = ScheduleExpressions.parse("R/2008-03-01/P1D", "PST") match {
        case Some(sch:JobSchedule) =>
          sch.recurrences must_== -1L
          sch.firstRun.getMillis must_== DateTime.parse("2008-03-01T00:00:00-08:00").getMillis
          //This is a hack because Period's equals seems broken!
          sch.period.toString must_== new Period(Days.ONE).toString
          false
        case _ =>
          true
      }
      fail must beEqualTo(false)
    }


  }
}
