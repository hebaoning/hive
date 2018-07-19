package org.apache.hive.hplsql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.sql.Date;
import java.util.Calendar;

@UDFType(deterministic = true)
public class DayOfYear extends GenericUDF {

  private ObjectInspector[] argumentsOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 0) {
      throw new UDFArgumentLengthException("At least one argument");
    }
    argumentsOI = arguments;
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Date dt = ((DateObjectInspector) argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get());
    Calendar c = Calendar.getInstance();
    c.setTime(dt);
    return new IntWritable(c.get(Calendar.DAY_OF_YEAR));
  }

  @Override
  public String getDisplayString(String[] children) {
    return "day_of_year";
  }

}