package org.apache.hive.hplsql.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hive.hplsql.Utils;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

@UDFType(deterministic = true)
public class ToDate extends GenericUDF {

  private ObjectInspector[] argumentsOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("At least two argument");
    }
    argumentsOI = arguments;
    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    try {
      String dtString = ((StringObjectInspector) argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get());
      String sqlFormat = ((StringObjectInspector) argumentsOI[1]).getPrimitiveJavaObject(arguments[1].get());
      String format = Utils.convertSqlDatetimeFormat(sqlFormat);
      long timeInMs = new SimpleDateFormat(format).parse(dtString).getTime();
      return new TimestampWritable(new Timestamp(timeInMs));
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "to_date";
  }

}

