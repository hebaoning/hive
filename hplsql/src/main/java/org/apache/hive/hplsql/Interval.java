/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.hplsql;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Date and time interval
 */
public class Interval {
  int days = 0;
  int milliseconds = 0;
  int months = 0;
  int hours = 0;
  int minutes = 0;
  int years = 0;
  
  /**
   * Add or subtract interval value to the specified date
   */
  public Date dateChange(Date in, boolean add) {
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(in.getTime());
    calendarChange(c, add);
    return new Date(c.getTimeInMillis());
  }
  
  /**
   * Add or subtract interval value to the specified timestamp
   */
  public Timestamp timestampChange(Timestamp in, boolean add) {
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(in.getTime());
    calendarChange(c, add);
    return new Timestamp(c.getTimeInMillis());
  }
  
  /**
   * Add interval value to the specified Calendar value
   */
  public Calendar calendarChange(Calendar c, boolean add) {
    int a = 1;
    if (!add) {
      a = -1;
    }
    if (days != 0) {
      c.add(Calendar.DAY_OF_MONTH, days * a);
    }
    if (milliseconds != 0) {
      c.setTimeInMillis(c.getTimeInMillis() + milliseconds * a);
    }
    if (months != 0) {
      addMonth(c, months * a);
    }
    if (hours != 0) {
      c.add(Calendar.HOUR, hours * a);
    }
    if (minutes != 0) {
      c.add(Calendar.MINUTE, minutes * a);
    }
    if (years != 0) {
      c.add(Calendar.YEAR, years * a);
    }
    return c;
  }
  
  /**
   * Set interval value
   */
  public Interval set(int value, String item) {
    if (item.compareToIgnoreCase("DAYS") == 0 || item.compareToIgnoreCase("DAY") == 0) {
      setDays(value);
    }
    if (item.compareToIgnoreCase("MICROSECONDS") == 0 || item.compareToIgnoreCase("MICROSECOND") == 0) {
      setMilliseconds(value);
    }
    if (item.compareToIgnoreCase("MONTHS") == 0 || item.compareToIgnoreCase("MONTH") == 0) {
      setMonths(value);
    }
    if (item.compareToIgnoreCase("HOURS") == 0 || item.compareToIgnoreCase("HOUR") == 0) {
      setHours(value);
    }
    if (item.compareToIgnoreCase("MINUTES") == 0 || item.compareToIgnoreCase("MINUTE") == 0) {
      setMinutes(value);
    }
    if (item.compareToIgnoreCase("YEARS") == 0 || item.compareToIgnoreCase("YEAR") == 0) {
      setYears(value);
    }
    return this;
  }
  
  /**
   * Set interval items
   */
  public void setDays(int days) {
    this.days = days;
  }
  
  public void setMilliseconds(int milliseconds) {
    this.milliseconds = milliseconds;
  }

  public void setMonths(int months) {
    this.months = months;
  }

  public void setHours(int hours) {
    this.hours = hours;
  }

  public void setMinutes(int minutes) {
    this.minutes = minutes;
  }

  public void setYears(int years) {
    this.years = years;
  }
  
  /**
   * Convert interval to string
   */
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    if (days != 0) {
      s.append(days);
      s.append(" days");
    }
    if (milliseconds != 0) {
      s.append(milliseconds);
      s.append(" milliseconds");
    }
    if (months != 0) {
      s.append(months);
      s.append(" months");
    }
    if (hours != 0) {
      s.append(hours);
      s.append(" hours");
    }
    if (minutes != 0) {
      s.append(minutes);
      s.append(" minutes");
    }
    if (years != 0) {
      s.append(years);
      s.append(" years");
    }
    return s.toString();
  }

  private Calendar addMonth(Calendar calendar, int numMonths) {
    boolean lastDatOfMonth = isLastDayOfMonth(calendar);
    calendar.add(Calendar.MONTH, numMonths);
    if (lastDatOfMonth) {
      int maxDd = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
      calendar.set(Calendar.DAY_OF_MONTH, maxDd);
    }
    return calendar;
  }

  private boolean isLastDayOfMonth(Calendar cal) {
    int maxDd = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
    int dd = cal.get(Calendar.DAY_OF_MONTH);
    return dd == maxDd;
  }
}
