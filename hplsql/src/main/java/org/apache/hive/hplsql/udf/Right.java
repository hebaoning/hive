package org.apache.hive.hplsql.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

@UDFType(deterministic = true)
public class Right extends GenericUDF {

  private ObjectInspector[] argumentsOI;
  private transient Converter[] converters = new Converter[1];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, NUMERIC_GROUP, VOID_GROUP);
    obtainStringConverter(arguments, 0, inputTypes, converters);
    argumentsOI = arguments;
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    String str = getStringValue(arguments, 0, converters);
    Integer len = (Integer)((IntObjectInspector)argumentsOI[1]).getPrimitiveJavaObject(arguments[1].get());
    return new Text(StringUtils.right(str, len));
  }

  @Override
  public String getDisplayString(String[] children) {
    return "right";
  }

}
