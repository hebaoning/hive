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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Description(name = "hplsql", value = "_FUNC_('query' [, :1, :2, ...n]) - Execute HPL/SQL query", extended = "Example:\n" + " > SELECT _FUNC_('CURRENT_DATE') FROM src LIMIT 1;\n")
@UDFType(deterministic = false)
public class Udf extends GenericUDF {

  private static final String FN_GET_STA_CODE = "fn_get_sta_code";
  private static final String FN_DAY_TO_RMB = "fn_day_to_rmb";
  private static final String FN_GET_TO_RMB_RATE = "fn_get_to_rmb_rate";
  private static final String GET_CODE_SPLIT = "get_code_split";
  private static final String GET_USD_EXCHANGE_RATE = "get_usd_exchange_rate";
  private static Map<String, Map<String, Object>> tableCache = new HashMap<>(2000);

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
    if (query.toLowerCase().startsWith(FN_GET_STA_CODE + "(")) {
      return evaluateFnGetStaCode(arguments);
    }
    if (query.toLowerCase().startsWith(FN_DAY_TO_RMB + "(")) {
      return evaluateFnDayToRmb(arguments);
    }
    if (query.toLowerCase().startsWith(FN_GET_TO_RMB_RATE + "(")) {
      return evaluateFnGetToRmbRate(arguments);
    }
    if (query.toLowerCase().startsWith(GET_CODE_SPLIT + "(")) {
      return evaluateGetCodeSplit(arguments);
    }
    if (query.toLowerCase().startsWith(GET_USD_EXCHANGE_RATE + "(")) {
      return evaluateGetUsdExchangeRate(arguments);
    }

    Var result = exec.run();
    if (result != null) {
      return result.toString();
    }
    return null;
  }

  Object evaluateFnGetStaCode(DeferredObject[] arguments) throws HiveException {
    Var v1 = exec.findVariable(":1");
    Var v2 = exec.findVariable(":2");
    if (v1 == null || v2 == null) {
      return null;
    }
    if (!tableCache.containsKey(FN_GET_STA_CODE)) {
      cacheTable(FN_GET_STA_CODE);
    }
    return tableCache.get(FN_GET_STA_CODE).get(v1.toString() + "||" + v2.toString());
  }

  Object evaluateFnDayToRmb(DeferredObject[] arguments) throws HiveException {
    String cyc = exec.findVariable(":2").toString().toUpperCase();
    Double amt = exec.findVariable(":3").doubleValue();

    if (cyc.equals("CNY") || cyc.equals("T01") || cyc.equals("R01")) {
      return amt;
    }
    if (!tableCache.containsKey(FN_DAY_TO_RMB)) {
      cacheTable(FN_DAY_TO_RMB);
    }
    if (!tableCache.get(FN_DAY_TO_RMB).containsKey(cyc)) {
      return 0;
    }
    return (Double)tableCache.get(FN_DAY_TO_RMB).get(cyc) * amt;
  }

  Object evaluateFnGetToRmbRate(DeferredObject[] arguments) throws HiveException {
    String cyc = exec.findVariable(":2").toString().toUpperCase();
    if (cyc.equals("CNY") || cyc.equals("T01") || cyc.equals("R01")) {
      return 1;
    }
    if (cyc.equals("U01")) {
      cyc = "USD";
    }
    if (!tableCache.containsKey(FN_GET_TO_RMB_RATE)) {
      cacheTable(FN_GET_TO_RMB_RATE);
    }
    if (!tableCache.get(FN_GET_TO_RMB_RATE).containsKey(cyc)) {
      return 0;
    }
    return tableCache.get(FN_GET_TO_RMB_RATE).get(cyc);
  }

  Object evaluateGetUsdExchangeRate(DeferredObject[] arguments) throws HiveException {
    String cyc = exec.findVariable(":2").toString().toUpperCase();
    Double amt = exec.findVariable(":3").doubleValue();

    if (cyc.equals("USD")) {
      return amt;
    }
    if (!tableCache.containsKey(GET_USD_EXCHANGE_RATE)) {
      cacheTable(GET_USD_EXCHANGE_RATE);
    }
    if (!tableCache.get(GET_USD_EXCHANGE_RATE).containsKey(cyc)) {
      return 0;
    }
    return (Double)tableCache.get(GET_USD_EXCHANGE_RATE).get(cyc) * amt;
  }

  Object evaluateGetCodeSplit(DeferredObject[] arguments) throws HiveException {
    if (!tableCache.containsKey(GET_CODE_SPLIT)) {
      cacheTable(GET_CODE_SPLIT);
    }

    String type = exec.findVariable(":1").toString();
    BigDecimal value = exec.findVariable(":2").decimalValue();
    List<List<Object>> table = (List<List<Object>>)tableCache.get(GET_CODE_SPLIT).get("table");
    for (List<Object> row: table) {
      String codeTypeId = (String) row.get(0);
      BigDecimal lmtDown = (BigDecimal) row.get(1);
      BigDecimal lmtUp = (BigDecimal) row.get(2);
      String code = (String) row.get(3);
      if (type.equals(codeTypeId) && value.compareTo(lmtDown) >= 1 && value.compareTo(lmtUp) <= 0) {
        return code;
      }
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
    String[] args = { "-e", query };
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

    switch (funcName) {
      case FN_GET_STA_CODE:
        cacheFnGetStaCode(funcName);
        break;
      case FN_DAY_TO_RMB:
        cacheFnDayToRmb(funcName);
        break;
      case FN_GET_TO_RMB_RATE:
        cacheFnGetToRmbRate(funcName);
        break;
      case GET_CODE_SPLIT:
        cacheGetCodeSplit(funcName);
        break;
      case GET_USD_EXCHANGE_RATE:
        cacheGetUsdExchangeRate(funcName);
        break;
    }
  }

  void cacheFnGetStaCode(String funcName) {
    String conn = exec.getStatementConnection();
    Query query = exec.executeQuery(null, "select sour_id, sour_code, code from sta_code_map", conn);
    try {
      ResultSet rs = query.getResultSet();
      while (rs.next()) {
        String key = rs.getString(1) + "||" + rs.getString(2);
        tableCache.get(funcName).putIfAbsent(key, rs.getString(3));
      }
    } catch (SQLException e) {
      exec.closeQuery(query, conn);
    }
  }

  void cacheFnDayToRmb(String funcName) {
    String conn = exec.getStatementConnection();
    String maxDate = exec.findVariable(":1").toString();
    String sqlFindMaxDate = String.format(
        "select max(yrcadate) from dds_ker_affmyrpt where yrcars1b='' and yrcadate<='%s'", maxDate);
    Query query = exec.executeQuery(null, sqlFindMaxDate, conn);
    try {
      ResultSet rs = query.getResultSet();
      while (rs.next()) {
        maxDate = rs.getString(1);
      }

      String sql = String.format(
          "select yrcaccyc, yrcaexrt/yrcacuno from dds_ker_affmyrpt where yrcars1b='' and yrcadate='%s'", maxDate);
      query = exec.executeQuery(null, sql, conn);
      rs = query.getResultSet();
      while (rs.next()) {
        tableCache.get(funcName).putIfAbsent(
            rs.getString(1).toUpperCase(),
            rs.getBigDecimal(2).doubleValue()
        );
      }
    } catch (SQLException e) {
      exec.closeQuery(query, conn);
    }
  }

  void cacheGetUsdExchangeRate(String funcName) {
    String conn = exec.getStatementConnection();
    String date = exec.findVariable(":1").toString();
    String sqlFindMinDate = String.format(
        "select min(sicadate) from nds_ker_affmsipm where fn_left(sicadate,6)=fn_left('%s',6) and sicars1b != '9'", date);
    String sqlFindMaxDate = String.format(
        "select max(sicadate) from nds_ker_affmsipm where sicadate<='%s' and sicars1b != '9'", date);

    Query query = null;
    try {
      String rateDate = null;
      for (String sql : new String[] {sqlFindMinDate, sqlFindMaxDate}) {
        query = exec.executeQuery(null, sql, conn);
        ResultSet rs = query.getResultSet();
        while (rs.next()) {
          rateDate = rs.getString(1);
        }
        if (rateDate != null) {
          break;
        }
      }

      String sql = String.format(
          "select sicaccyc, sicaexrt/sicacuno from nds_ker_affmsipm where sicars1b != '9' and sicadate='%s'", rateDate);
      query = exec.executeQuery(null, sql, conn);
      ResultSet rs = query.getResultSet();
      while (rs.next()) {
        tableCache.get(funcName).putIfAbsent(
            rs.getString(1).toUpperCase(),
            rs.getBigDecimal(2).doubleValue()
        );
      }
    } catch (SQLException e) {
      exec.closeQuery(query, conn);
    }
  }

  void cacheFnGetToRmbRate(String funcName) {
    String conn = exec.getStatementConnection();
    String maxDate = exec.findVariable(":1").toString();
    String sqlFindMaxDate = String.format(
        "select max(yrcadate) from dds_ker_affmyrpt where yrcars1b='' and yrcadate<='%s'", maxDate);
    Query query = exec.executeQuery(null, sqlFindMaxDate, conn);
    try {
      ResultSet rs = query.getResultSet();
      while (rs.next()) {
        maxDate = rs.getString(1);
      }

      String sql = String.format(
          "select yrcaccyc, yrcaexrt/yrcacuno from dds_ker_affmyrpt where yrcars1b='' and yrcadate='%s'", maxDate);
      query = exec.executeQuery(null, sql, conn);
      rs = query.getResultSet();
      while (rs.next()) {
        tableCache.get(funcName).putIfAbsent(
            rs.getString(1).toUpperCase(),
            rs.getBigDecimal(2).doubleValue()
        );
      }
    } catch (SQLException e) {
      exec.closeQuery(query, conn);
    }
  }

  void cacheGetCodeSplit(String funcName) {
    String conn = exec.getStatementConnection();
    Query query = exec.executeQuery(null, "select code_type_id, lmt_down, lmt_up, code from sta_code_split", conn);
    try {
      ResultSet rs = query.getResultSet();
      List<List<Object>> table = new ArrayList<>();
      while (rs.next()) {
        ArrayList<Object> row = new ArrayList<>();
        row.add(rs.getString(1));
        row.add(rs.getBigDecimal(2));
        row.add(rs.getBigDecimal(3));
        row.add(rs.getString(4));
        table.add(row);
      }
      tableCache.get(funcName).putIfAbsent("table", table);
    } catch (SQLException e) {
      exec.closeQuery(query, conn);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "hplsql";
  }
}
