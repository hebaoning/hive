package org.apache.hive.hplsql.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@UDFType(deterministic = true)
@Deprecated
public class Decode extends GenericUDF {

  private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  private ObjectInspector[] argumentsOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 3) {
      throw new UDFArgumentLengthException("At least 3 arguments");
    }
    argumentsOI = arguments;
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    returnOIResolver.update(arguments[2]);
    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    int i;
    Object obj1 = returnOIResolver.convertIfNecessary(arguments[0].get(), argumentsOI[0]);
    for (i = 1; i < arguments.length - 1; i += 2) {
      Object obj2 = returnOIResolver.convertIfNecessary(arguments[i].get(), argumentsOI[0]);
      if (obj1.equals(obj2)) {
        return returnOIResolver.convertIfNecessary(arguments[i+1].get(), argumentsOI[2]);
      }
    }
    if (i == arguments.length - 1) {
      return returnOIResolver.convertIfNecessary(arguments[i].get(), argumentsOI[2]);
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "decode";
  }

}