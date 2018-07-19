package org.apache.hive.hplsql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@UDFType(deterministic = true)
public class Decode extends GenericUDF {

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 3) {
      throw new UDFArgumentLengthException("At least 3 arguments");
    }
    // FIXME
    return arguments[2];
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    int i = 1;
    for (i = 1; i < arguments.length - 1; i += 2) {
      if (arguments[0].get().equals(arguments[i].get())) {
        return arguments[i+1].get();
      }
    }
    if (i == arguments.length - 1) {
      return arguments[i].get();
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "decode";
  }

}