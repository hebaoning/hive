package org.apache.hive.hplsql.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Date;

@UDFType(deterministic = true)
public class FnToDate extends GenericUDF {

  private ObjectInspector[] argumentsOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 0) {
      throw new UDFArgumentLengthException("At least one argument");
    }
    argumentsOI = arguments;
    return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return new DateWritable(new Date(
          LocalDateTime.parse("1899-12-31").toDateTime().getMillis()));
    }
    String var = ((StringObjectInspector)argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get());
    var = StringUtils.stripEnd(var, " ");
    if (var.length() != 8) {
      return new DateWritable(new Date(
          LocalDateTime.parse("1899-12-31").toDateTime().getMillis()));
    }
    return new DateWritable(new Date(
        LocalDateTime.parse(var, DateTimeFormat.forPattern("yyyyMMdd")).toDateTime().getMillis()));
  }

  @Override
  public String getDisplayString(String[] children) {
    return "fn_to_date";
  }

}

