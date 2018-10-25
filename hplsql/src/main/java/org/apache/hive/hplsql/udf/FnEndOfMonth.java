package org.apache.hive.hplsql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hive.hplsql.Utils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

@UDFType(deterministic = true)
public class FnEndOfMonth extends GenericUDF {

  private ObjectInspector[] argumentsOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 0) {
      throw new UDFArgumentLengthException("At least one argument");
    }
    argumentsOI = arguments;
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    String var = ((StringObjectInspector)argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get());
    LocalDate dt;
    if (var.length() == 8) {
      dt = LocalDate.parse(var, ISODateTimeFormat.basicDate());
    } else {
      dt = LocalDate.parse(var);
    }
    LocalDate lastDate = dt.dayOfMonth().withMaximumValue();
    return new Text(lastDate.toString("yyyyMMdd"));
  }

  @Override
  public String getDisplayString(String[] children) {
    return "fn_end_of_month";
  }

}