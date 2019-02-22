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

package org.apache.hive.hplsql.functions;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.hplsql.*;
import org.joda.time.LocalDateTime;
import org.joda.time.format.ISODateTimeFormat;

public class FunctionDatetime extends Function {

  private static final int DAYS_FROM_Y1_TO_Y1900 = 693595;

  public FunctionDatetime(Exec e) {
    super(e);
  }

  /** 
   * Register functions
   */
  @Override
  public void register(Function f) {
    f.map.put("DATE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { date(ctx); }});
    f.map.put("FROM_UNIXTIME", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { fromUnixtime(ctx); }});
    f.map.put("NOW", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { now(ctx); }});
    f.map.put("TIMESTAMP_ISO", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { timestampIso(ctx); }});
    f.map.put("TO_TIMESTAMP", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { toTimestamp(ctx); }});
    f.map.put("TIMESTAMP_FORMAT", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { toTimestamp(ctx); }});
    f.map.put("TO_DATE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { toTimestamp(ctx); }});
    f.map.put("TS_FMT", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { toTimestamp(ctx); }});
    f.map.put("UNIX_TIMESTAMP", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { unixTimestamp(ctx); }});
    f.map.put("TRUNC", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { trunc(ctx); }});
    f.map.put("DAYOFWEEK", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { dayOfWeek(ctx); }});
    f.map.put("DAYOFYEAR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { dayOfYear(ctx); }});
    f.map.put("YEAR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { year(ctx); }});
    f.map.put("MONTH", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { month(ctx); }});
    f.map.put("DAY", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { day(ctx); }});
    f.map.put("_DAYS", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { days(ctx); }});

    f.specMap.put("CURRENT_DATE", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentDate(ctx); }});
    f.specMap.put("CURRENT_TIMESTAMP", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentTimestamp(ctx); }});
    f.specMap.put("SYSDATE", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentTimestamp(ctx); }});

    f.specSqlMap.put("CURRENT_DATE", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentDateSql(ctx); }});
    f.specSqlMap.put("CURRENT_TIMESTAMP", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { currentTimestampSql(ctx); }});
 }
  
  /**
   * CURRENT_DATE
   */
  public void currentDate(HplsqlParser.Expr_spec_funcContext ctx) {
    evalVar(currentDate());
  }
  
  public static Var currentDate() {
    SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
    String s = f.format(Calendar.getInstance().getTime());
    return new Var(Var.Type.DATE, Utils.toDate(s)); 
  }
  
  /**
   * CURRENT_DATE in executable SQL statement
   */
  public void currentDateSql(HplsqlParser.Expr_spec_funcContext ctx) {
    if (exec.getConnectionType() == Conn.Type.HIVE) {
      evalString("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))");
    } 
    else {
      evalString(exec.getFormattedText(ctx));
    }
  }
  
  /**
   * CURRENT_TIMESTAMP
   */
  public void currentTimestamp(HplsqlParser.Expr_spec_funcContext ctx) {
    int precision = evalPop(ctx.expr(0), 3).intValue();
    evalVar(currentTimestamp(precision));
  }
  
  public static Var currentTimestamp(int precision) {
    String format = "yyyy-MM-dd HH:mm:ss";
    if (precision > 0 && precision <= 3) {
      format += "." + StringUtils.repeat("S", precision);
    }
    SimpleDateFormat f = new SimpleDateFormat(format);
    String s = f.format(Calendar.getInstance(TimeZone.getDefault()).getTime());
    return new Var(Utils.toTimestamp(s), precision); 
  }
  
  /**
   * CURRENT_TIMESTAMP in executable SQL statement
   */
  public void currentTimestampSql(HplsqlParser.Expr_spec_funcContext ctx) {
    if (exec.getConnectionType() == Conn.Type.HIVE) {
      evalString("FROM_UNIXTIME(UNIX_TIMESTAMP())");
    } 
    else {
      evalString(exec.getFormattedText(ctx));
    }
  }
  
  /**
   * DATE function
   */
  void date(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    Var var = new Var(Var.Type.DATE);
    var.cast(evalPop(ctx.func_param(0).expr()));
    evalVar(var);
  }
  
  /**
   * NOW() function (current date and time)
   */
  void now(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx != null) {
      evalNull();
      return;
    }
    evalVar(currentTimestamp(3));
  }

  /**
   * TIMESTAMP_ISO function
   */
  void timestampIso(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    Var var = new Var(Var.Type.TIMESTAMP);
    var.cast(evalPop(ctx.func_param(0).expr()));
    evalVar(var);
  }
  
  /**
   * TO_TIMESTAMP function
   */
  void toTimestamp(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 2) {
      evalNull();
      return;
    }
    try {
      String value = evalPop(ctx.func_param(0).expr()).toString();
      String sqlFormat = evalPop(ctx.func_param(1).expr()).toString();
      String format = Utils.convertSqlDatetimeFormat(sqlFormat);
      long timeInMs = new SimpleDateFormat(format).parse(value).getTime();
      evalVar(new Var(Var.Type.TIMESTAMP, new Timestamp(timeInMs)));
    }
    catch (Exception e) {
      // exec.signal(e);
      trace(ctx, "Exception: " + e.getMessage());
      evalNull();
    }
  }
  
  /**
   * FROM_UNIXTIME() function (convert seconds since 1970-01-01 00:00:00 to timestamp)
   */
  void fromUnixtime(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt == 0) {
      evalNull();
      return;
    }
    long epoch = evalPop(ctx.func_param(0).expr()).longValue();
    String format = "yyyy-MM-dd HH:mm:ss";
    if (cnt > 1) {
      format = evalPop(ctx.func_param(1).expr()).toString();
    }
    evalString(new SimpleDateFormat(format).format(new Date(epoch * 1000)));
  }
  
  /**
   * UNIX_TIMESTAMP() function (current date and time in seconds since 1970-01-01 00:00:00)
   */
  void unixTimestamp(HplsqlParser.Expr_func_paramsContext ctx) {
    evalVar(new Var(System.currentTimeMillis()/1000));
  }

  /**
   * TRUNC function
   */
  void trunc(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx == null) {
      evalNull();
      return;
    }

    if (ctx.func_param().size() == 1) {
      eval(ctx);
      return;
    }

    Var v1 = evalPop(ctx.func_param(0).expr());
    if (v1.type == Var.Type.TIMESTAMP) {
      String sqlFormat = evalPop(ctx.func_param(1).expr()).toString();
      Date d = truncTimestamp(v1.timestampValue(), sqlFormat);
      if (d != null) {
        evalVar(new Var(new Timestamp(d.getTime()), 0));
        return;
      }
    }

    evalNull();
  }

  /**
   * DAYOFWEEK() function
   *   returns an integer, in the range of 1 to 7, that represents the day of the week,
   *   where 1 is Sunday and 7 is Saturday.
   */
  private void dayOfWeek(HplsqlParser.Expr_func_paramsContext ctx) {
    Integer v = getPartOfDate(ctx, Calendar.DAY_OF_WEEK);
    if (v != null) {
      evalInt(v);
    }
    else {
      evalNull();
    }
  }

  private void dayOfYear(HplsqlParser.Expr_func_paramsContext ctx) {
    Integer v = getPartOfDate(ctx, Calendar.DAY_OF_YEAR);
    if (v != null) {
      evalInt(v);
    }
    else {
      evalNull();
    }
  }

  private void year(HplsqlParser.Expr_func_paramsContext ctx) {
    Integer v = getPartOfDate(ctx, Calendar.YEAR);
    if (v != null) {
      evalInt(v);
    }
    else {
      evalNull();
    }
  }

  private void month(HplsqlParser.Expr_func_paramsContext ctx) {
    Integer v = getPartOfDate(ctx, Calendar.MONTH);
    if (v != null) {
      evalInt(v + 1);
    }
    else {
      evalNull();
    }
  }

  private void day(HplsqlParser.Expr_func_paramsContext ctx) {
    Integer v = getPartOfDate(ctx, Calendar.DAY_OF_MONTH);
    if (v != null) {
      evalInt(v);
    }
    else {
      evalNull();
    }
  }

  /**
   * @return Days between @day and 0001-1-1
   */
  private void days(HplsqlParser.Expr_func_paramsContext ctx) {
    Var v = evalPop(ctx.func_param(0).expr());
    if (v != null) {
      Long days = (v.dateValue().getTime() - java.sql.Date.valueOf("1900-01-01").getTime()) / TimeUnit.DAYS.toMillis(1);
      evalInt(days.intValue() + DAYS_FROM_Y1_TO_Y1900);
    }
    else {
      evalNull();
    }
  }

  private Integer getPartOfDate(HplsqlParser.Expr_func_paramsContext ctx, int part) {
    if (ctx == null) {
      return null;
    }

    Var v = evalPop(ctx.func_param(0).expr());
    Calendar c = Calendar.getInstance();
    if (v.type == Var.Type.DATE) {
      c.setTime(v.dateValue());
    } else if (v.type == Var.Type.TIMESTAMP) {
      c.setTime(v.timestampValue());
    } else if (v.type == Var.Type.STRING) {
      java.sql.Date dt;
      if (v.toString().length() == 8) {
        dt = new java.sql.Date(
            LocalDateTime.parse(v.toString(), ISODateTimeFormat.basicDate()).toDateTime().getMillis());
      } else {
        dt = new java.sql.Date(LocalDateTime.parse(v.toString()).toDateTime().getMillis());
      }
      c.setTime(dt);
    } else {
      return null;
    }
    return c.get(part);
  }

  private Date truncTimestamp(Timestamp date, String sqlFormat) {
    try {
      Calendar c = Calendar.getInstance();
      c.setTime(date);
      switch (sqlFormat.toLowerCase()) {
        case "yyyy":
        case "yy":
          c.set(Calendar.DAY_OF_YEAR, 1);
          resetDay(c);
          break;
        case "mm":
          c.set(Calendar.DAY_OF_MONTH, 1);
          resetDay(c);
          break;
        case "dd":
          resetDay(c);
          break;
        case "d":
          c.set(Calendar.DAY_OF_WEEK, 1);
          resetDay(c);
          break;
        case "hh":
          c.set(Calendar.MINUTE, 0);
          c.set(Calendar.SECOND, 0);
          break;
        case "mi":
          c.set(Calendar.SECOND, 0);
          break;
      }
      return c.getTime();
    } catch (Exception e) {
      exec.signal(e);
    }
    return null;
  }

  private void resetDay(Calendar calendar) {
    calendar.set(Calendar.HOUR, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
  }
}  
