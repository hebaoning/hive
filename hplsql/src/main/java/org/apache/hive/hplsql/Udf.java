/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.hplsql;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Description(name = "hplsql", value = "_FUNC_('query' [, :1, :2, ...n]) - Execute HPL/SQL query", extended = "Example:\n" + " > SELECT _FUNC_('CURRENT_DATE') FROM src LIMIT 1;\n")
@UDFType(deterministic = false)
public class Udf extends GenericUDF {

  static Map<String, Map<String, String>> tableCache = new HashMap<>(2000);

  Exec exec;
  StringObjectInspector queryOI;
  ObjectInspector[] argumentsOI;
  
  /**
   * Initialize UDF
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 0) {
      throw new UDFArgumentLengthException("At least one argument must be specified");
    }    
    if (!(arguments[0] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("First argument must be a string");
    }
    queryOI = (StringObjectInspector)arguments[0];
    argumentsOI = arguments;
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }
  
  /**
   * Execute UDF
   */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (exec == null) {
      initExec(arguments);
    }
    if (arguments.length > 1) {
      setParameters(arguments);
    }

    String query = queryOI.getPrimitiveJavaObject(arguments[0].get());
    if (query.toLowerCase().startsWith("fn_get_sta_code(")) {
      if (!tableCache.containsKey("fn_get_sta_code")) {
        cacheTable("fn_get_sta_code");
      }
      return tableCache.get("fn_get_sta_code").get(
            exec.findVariable(":1").toString() + "||" + exec.findVariable(":2").toString());
    }

    Var result = exec.run();
    if (result != null) {
      return result.toString();
    }
    return null;
  }

  /**
   * init exec
   */
  public void initExec(DeferredObject[] arguments) throws HiveException {
    exec = new Exec();
    exec.enterGlobalScope();
    String query = queryOI.getPrimitiveJavaObject(arguments[0].get());
    String[] args = { "-e", query, "-trace" };
    try {
      exec.setUdfRun(true);
      exec.init(args);
    } catch (Exception e) {
      throw new HiveException(e.getMessage());
    }
  }
  
  /**
   * Set parameters for the current call
   */
  void setParameters(DeferredObject[] arguments) throws HiveException {
    for (int i = 1; i < arguments.length; i++) {
      String name = ":" + i;      
      if (argumentsOI[i] instanceof StringObjectInspector) {
        String value = ((StringObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, value);
        }        
      }
      else if (argumentsOI[i] instanceof IntObjectInspector) {
        Integer value = (Integer)((IntObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, new Var(new Long(value)));
        }        
      }
      else if (argumentsOI[i] instanceof LongObjectInspector) {
        Long value = (Long)((LongObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, new Var(value));
        }        
      }
      else if (argumentsOI[i] instanceof BooleanObjectInspector) {
        Boolean value = (Boolean)((BooleanObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, new Var(value));
        }
      }
      else if (argumentsOI[i] instanceof HiveDecimalObjectInspector) {
        HiveDecimal value = ((HiveDecimalObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, new Var(value.bigDecimalValue()));
        }
      }
      else if (argumentsOI[i] instanceof DateObjectInspector) {
        Date value = ((DateObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, new Var(value));
        }
      }
      else if (argumentsOI[i] instanceof TimestampObjectInspector) {
        Timestamp value = ((TimestampObjectInspector)argumentsOI[i]).getPrimitiveJavaObject(arguments[i].get());
        if (value != null) {
          exec.setVariable(name, new Var(value, 3));
        }
      }
      else {
        // exec.setVariableToNull(name);
        throw new HiveException("invalid type " + argumentsOI[i].getTypeName());
      }
    }
  }

  void cacheTable(String funcName) {
    funcName = funcName.toLowerCase();
    tableCache.put(funcName, new HashMap<>());

    String conn = exec.getStatementConnection();
    Query query = exec.executeQuery(null, "select sour_id, sour_code, code from sta_code_map", conn);
    try {
      ResultSet rs = query.getResultSet();
      while (rs.next()) {
        tableCache.get(funcName).put(rs.getString(1) + "||" + rs.getString(2), rs.getString(3));
      }
    } catch (SQLException e) {
      exec.closeQuery(query, conn);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "hplsql";
  }
}
