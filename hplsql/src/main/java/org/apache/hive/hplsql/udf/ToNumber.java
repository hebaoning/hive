package org.apache.hive.hplsql.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

@UDFType(deterministic = true)
public class ToNumber extends GenericUDF {

  private ObjectInspector[] argumentsOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 0) {
      throw new UDFArgumentLengthException("At least one argument");
    }
    argumentsOI = arguments;
    DecimalTypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(
        HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    String var = ((StringObjectInspector)argumentsOI[0]).getPrimitiveJavaObject(arguments[0].get());
    return new HiveDecimalWritable(HiveDecimal.create(var));
  }

  @Override
  public String getDisplayString(String[] children) {
    return "to_number";
  }

}

