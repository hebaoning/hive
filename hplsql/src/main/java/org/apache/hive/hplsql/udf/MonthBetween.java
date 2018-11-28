package org.apache.hive.hplsql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Calendar;
import java.util.Date;

import static java.util.Calendar.DATE;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.CHAR;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.VARCHAR;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(name = "month_between", value = "_FUNC_(date1, date2) "
    + "- returns number of months between dates date1 and date2",
    extended = "If date1 is later than date2, then the result is positive. "
        + "If date1 is earlier than date2, then the result is negative. "
        + "If date1 and date2 are either the same days of the month or both last days of months, "
        + "then the result is always an integer. "
        + "Otherwise the UDF calculates the fractional portion of the result based on a 31-day "
        + "month and considers the difference in time components date1 and date2.\n"
        + "date1 and date2 type can be date, timestamp or string in the format "
        + "'yyyy-MM-dd' or 'yyyy-MM-dd HH:mm:ss'. "
        + "The result is rounded to 8 decimal places by default. Set roundOff=false otherwise.\n"
        + " Example:\n"
        + "  > SELECT _FUNC_('1997-02-28 10:30:00', '1996-10-30');\n 3.94959677")
public class MonthBetween extends GenericUDF {
  private transient ObjectInspectorConverters.Converter[] tsConverters = new ObjectInspectorConverters.Converter[2];
  private transient PrimitiveObjectInspector.PrimitiveCategory[] tsInputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];
  private transient ObjectInspectorConverters.Converter[] dtConverters = new ObjectInspectorConverters.Converter[2];
  private transient PrimitiveObjectInspector.PrimitiveCategory[] dtInputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];
  private final Calendar cal1 = Calendar.getInstance();
  private final Calendar cal2 = Calendar.getInstance();
  private final IntWritable output = new IntWritable();
  private boolean isRoundOffNeeded = true;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    // the function should support both short date and full timestamp format
    // time part of the timestamp should not be skipped
    checkArgGroups(arguments, 0, tsInputTypes, STRING_GROUP, DATE_GROUP);
    checkArgGroups(arguments, 1, tsInputTypes, STRING_GROUP, DATE_GROUP);

    checkArgGroups(arguments, 0, dtInputTypes, STRING_GROUP, DATE_GROUP);
    checkArgGroups(arguments, 1, dtInputTypes, STRING_GROUP, DATE_GROUP);

    obtainTimestampConverter(arguments, 0, tsInputTypes, tsConverters);
    obtainTimestampConverter(arguments, 1, tsInputTypes, tsConverters);

    obtainDateConverter(arguments, 0, dtInputTypes, dtConverters);
    obtainDateConverter(arguments, 1, dtInputTypes, dtConverters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String date1Str = dtConverters[0].convert(arguments[0].get()).toString();
    String date2Str = dtConverters[1].convert(arguments[1].get()).toString();
    if (date1Str.length() != 8 || date2Str.length() != 8) {
      output.set(-2);
      return output;
    }

    LocalDateTime dt1 = LocalDateTime.parse(date1Str, DateTimeFormat.forPattern("yyyyMMdd"));
    int day1Max = dt1.dayOfMonth().getMaximumValue();
    Date date1 = dt1.toDate();

    LocalDateTime dt2 = LocalDateTime.parse(date2Str, DateTimeFormat.forPattern("yyyyMMdd"));
    int day2Max = dt2.dayOfMonth().getMaximumValue();
    Date date2 = dt2.toDate();

    if (date1.compareTo(date2) < 0) {
      output.set(-1);
      return output;
    }

    cal1.setTime(date1);
    cal2.setTime(date2);

    int monDiffInt = (cal1.get(YEAR) - cal2.get(YEAR)) * 12 + (cal1.get(MONTH) - cal2.get(MONTH));
    int day1 = cal1.get(Calendar.DAY_OF_MONTH);
    int day2 = cal2.get(Calendar.DAY_OF_MONTH);
    int dayDiffInt = 0;
    if (day2Max - day2 + day1 - day1Max == 0 || day1 - day2 == 0) {
      dayDiffInt = 0;
    } else {
      dayDiffInt = day1 - day2;
    }

    output.set(monDiffInt + (dayDiffInt <= 0 ? 0 : 1));
    return output;
  }

  protected int getDayPartInSec(Calendar cal) {
    int dd = cal.get(DATE);
    int HH = cal.get(HOUR_OF_DAY);
    int mm = cal.get(MINUTE);
    int ss = cal.get(SECOND);
    int dayInSec = dd * 86400 + HH * 3600 + mm * 60 + ss;
    return dayInSec;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "month_between";
  }
}
