package org.apache.hive.hplsql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hive.hplsql.Utils;
import org.joda.time.LocalDateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

@UDFType(deterministic = true)
public class ToChar extends GenericUDF {

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
    if (arguments.length == 1) {
      return new Text(
          ((PrimitiveObjectInspector)argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get()).toString());
    }
    if (argumentsOI[0] instanceof DateObjectInspector
        || argumentsOI[0] instanceof TimestampObjectInspector
        || argumentsOI[0] instanceof StringObjectInspector) {
      String sqlFormat = ((StringObjectInspector)argumentsOI[1]).getPrimitiveJavaObject(arguments[1].get());
      String format = Utils.convertSqlDatetimeFormat(sqlFormat);
      if (argumentsOI[0] instanceof StringObjectInspector) {
        Date dt;
        String str = ((StringObjectInspector) argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get());
        if (str.length() == 8) {
          dt = new Date(LocalDateTime.parse(str, ISODateTimeFormat.basicDate()).toDateTime().getMillis());
        } else {
          dt = new Date(LocalDateTime.parse(str).toDateTime().getMillis());
        }
        return new Text(new SimpleDateFormat(format).format(dt));
      }
      if (argumentsOI[0] instanceof DateObjectInspector) {
        Date t = ((DateObjectInspector) argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get());
        return new Text(new SimpleDateFormat(format).format(t));
      }
      Timestamp t = ((TimestampObjectInspector) argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get());
      return new Text(new SimpleDateFormat(format).format(t));
    }
    throw new HiveException("Unsupported arg type " + argumentsOI[0].getTypeName());
  }

  @Override
  public String getDisplayString(String[] children) {
    return "to_char";
  }

}