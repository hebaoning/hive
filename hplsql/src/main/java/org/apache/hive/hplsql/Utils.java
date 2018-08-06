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

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

  public static final String REGULAR_COLUMN = "rcol";
  public static final String PARTITION_COLUMN = "pcol";

  /**
   * Unquote string and remove escape characters inside the script 
   */
  public static String unquoteString(String s) {
	  if(s == null) {
		  return null;
	  }
    s = s.replaceAll("^['\"]|['\"]$", "")
        .replaceAll("''|\\'", "'");
    return s;
  }

  /**
   * Quote string and escape characters - ab'c -> 'ab''c'
   */
  public static String quoteString(String s) {
    if(s == null) {
      return null;
    }    
    int len = s.length();
    StringBuilder s2 = new StringBuilder(len + 2).append('\'');
    
    for (int i = 0; i < len; i++) {
      char ch = s.charAt(i);
      s2.append(ch);
      if(ch == '\'') {
        s2.append(ch);
      }      
    }
    s2.append('\'');
    return s2.toString();
  }
  
  /**
   * Merge quoted strings: 'a' 'b' -> 'ab'; 'a''b' 'c' -> 'a''bc'
   */
  public static String mergeQuotedStrings(String s1, String s2) {
	  if(s1 == null || s2 == null) {
		  return null;
	  }
	  
	  int len1 = s1.length();
	  int len2 = s2.length();
	  
	  if(len1 == 0 || len2 == 0) {
		  return s1;
	  }
	  
	  return s1.substring(0, len1 - 1) + s2.substring(1);
  }
  
  /**
   * Convert String to Date
   */
  public static Date toDate(String s) {
    int len = s.length();
    if(len >= 10) {
      int c4 = s.charAt(4);
      int c7 = s.charAt(7);
      // YYYY-MM-DD
      if(c4 == '-' && c7 == '-') {
        return Date.valueOf(s.substring(0, 10));
      }
    }
    return null;    
  }
  
  /**
   * Convert String to Timestamp
   */
  public static Timestamp toTimestamp(String s) {
    int len = s.length();
    if(len >= 10) {
      int c4 = s.charAt(4);
      int c7 = s.charAt(7);
      // YYYY-MM-DD 
      if(c4 == '-' && c7 == '-') {
        // Convert DB2 syntax: YYYY-MM-DD-HH.MI.SS.FFF
        if(len > 19) {
          if(s.charAt(10) == '-') {
            String s2 = s.substring(0, 10) + ' ' + s.substring(11, 13) + ':' + s.substring(14, 16) + ':' + 
                s.substring(17);
            return Timestamp.valueOf(s2);
          }          
        }
        else if(len == 10) {
          s += " 00:00:00.000";
        }
        return Timestamp.valueOf(s);
      }
    }
    return null;    
  }
  
  /**
   * Compare two String values and return min or max 
   */
  public static String minMaxString(String s1, String s2, boolean max) {
    if(s1 == null) {
      return s2;
    } 
    else if(s2 == null) {
      return s1;
    }    
    int cmp = s1.compareTo(s2);
    if((max && cmp < 0) || (!max && cmp > 0)) {
      return s2;
    }
    return s1;
  }

  /**
   * Compare two Int values and return min or max 
   */
  public static Long minMaxInt(Long i1, String s, boolean max) {
    Long i2 = null;
    try {
      i2 = Long.parseLong(s);
    }
    catch(NumberFormatException e) {}
    if(i1 == null) {
      return i2;
    } 
    else if(i2 == null) {
      return i1;
    }    
    if((max && i1.longValue() < i2.longValue()) || (!max && i1.longValue() > i2.longValue())) {
      return i2;
    }
    return i1;
  }
  
  /**
   * Compare two Date values and return min or max 
   */
  public static Date minMaxDate(Date d1, String s, boolean max) {
    Date d2 = Utils.toDate(s);
    if(d1 == null) {
      return d2;
    } else if(d2 == null) {
      return d1;
    }    
    if((max && d1.before(d2)) || (!max && d1.after(d2))) {
      return d2;
    }
    return d1;
  }
  
  /**
   * Convert String array to a string with the specified delimiter
   */
  public static String toString(String[] a, char del) {
    StringBuilder s = new StringBuilder();
    for(int i=0; i < a.length; i++) {
      if(i > 0) {
        s.append(del);
      }
      s.append(a[i]);
    }
    return s.toString();
  }
  
  /**
   * Convert SQL datetime format string to Java SimpleDateFormat
   */
  public static String convertSqlDatetimeFormat(String in) {
    StringBuilder out = new StringBuilder();
    int len = in.length();
    int i = 0;
    while (i < len) {
      if (i + 4 <= len && in.substring(i, i + 4).compareTo("YYYY") == 0) {
        out.append("yyyy");
        i += 4;
      }
      else if (i + 2 <= len && in.substring(i, i + 2).compareTo("mm") == 0) {
        out.append("MM");
        i += 2;
      }
      else if (i + 2 <= len && in.substring(i, i + 2).compareTo("DD") == 0) {
        out.append("dd");
        i += 2;
      }
      else if (i + 4 <= len && in.substring(i, i + 4).compareToIgnoreCase("HH24") == 0) {
        out.append("HH");
        i += 4;
      }
      else if (i + 2 <= len && in.substring(i, i + 2).compareToIgnoreCase("MI") == 0) {
        out.append("mm");
        i += 2;
      }
      else if (i + 2 <= len && in.substring(i, i + 2).compareTo("SS") == 0) {
        out.append("ss");
        i += 2;
      }
      else {
        out.append(in.charAt(i));
        i++;
      }
    }
    return out.toString();
  }
  
  /**
   * Get the executable directory
   */
  public static String getExecDir() {
    String dir = Hplsql.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    if (dir.endsWith(".jar")) {   
      dir = dir.substring(0, dir.lastIndexOf("/") + 1);
    }
    return dir;
  }
  
  /**
   * Format size value specified in bytes
   */
  public static String formatSizeInBytes(long bytes, String postfix) {
    String out; 
    if (bytes == 1) {
      out = bytes + " byte";
    }
    else if (bytes < 1024) {
      out = bytes + " bytes";
    }
    else if (bytes < 1024 * 1024) {
      out = String.format("%.1f", ((float)bytes)/1024) + " KB";
    }
    else if (bytes < 1024 * 1024 * 1024) {
      out = String.format("%.1f", ((float)bytes)/(1024 * 1024)) + " MB";
    }
    else {
      out = String.format("%.1f", ((float)bytes)/(1024 * 1024 * 1024)) + " GB";
    }
    if (postfix != null && !postfix.isEmpty()) {
      out += postfix;
    }
    return out;
  }
  
  public static String formatSizeInBytes(long bytes) {
    return Utils.formatSizeInBytes(bytes, null);
  }
  
  /**
   * Format elasped time
   */
  public static String formatTime(long msElapsed) {
    if (msElapsed < 60000) {
      return msElapsed/1000 + " sec";
    }
    else if (msElapsed < 60000 * 60) {
      return msElapsed/60000 + " min " + (msElapsed%60000)/1000 + " sec";
    }
    return "";
  }
  
  /**
   * Format bytes per second rate
   */
  public static String formatBytesPerSec(long bytes, long msElapsed) {
    if (msElapsed < 30) {
      return "n/a";
    }
    float bytesPerSec = ((float)bytes)/msElapsed*1000;
    return Utils.formatSizeInBytes((long)bytesPerSec, "/sec");
  }
  
  /**
   * Format percentage
   */
  public static String formatPercent(long current, long all) {
    return String.format("%.1f", ((float)current)/all*100) + "%";
  }
  
  /**
   * Format count
   */
  public static String formatCnt(long value, String suffix) {
    if (value == 1) {
      return value + " " + suffix; 
    }
    return value + " " + suffix + "s";
  }
  
  public static String formatCnt(long value, String suffix, String suffix2) {
    if (value == 1) {
      return value + " " + suffix;
    }
    return value + " " + suffix2;
  }
  
  /**
   * Note. This stub is to resolve name conflict with ANTLR generated source using org.antlr.v4.runtime.misc.Utils.join
   */
  static <T> String join(T[] array, String separator) {
    return org.antlr.v4.runtime.misc.Utils.join(array, separator);
  }

  /**
   *
   */
  public static List<String> getColumnNames(Exec exec, ParserRuleContext ctx, String tableName) {
    Query q = exec.executeQuery(ctx, "SHOW COLUMNS IN " + tableName, exec.conf.defaultConnection);
    if (q.error()) {
      exec.signal(q);
      return null;
    }
    exec.setSqlSuccess();
    ResultSet rs = q.getResultSet();
    if (rs == null) {
      return null;
    }

    List<String> columnNames = new ArrayList<>();
    try {
      while (rs.next()) {
        columnNames.add(rs.getString(1));
      }
      exec.trace(ctx, tableName + " columns: " + StringUtils.join(columnNames, ", "));
    } catch (SQLException e) {
      columnNames.clear();
      exec.trace(ctx, e.getMessage());
    }
    exec.closeQuery(q, exec.conf.defaultConnection);
    return columnNames;
  }

  /**
   * Return <column type, column name list>
   */
  public static Map<String, List<String>> getColumnInfo(Exec exec, ParserRuleContext ctx, String tableName) {
    Query q = exec.executeQuery(ctx, "DESCRIBE " + tableName, exec.conf.defaultConnection);
    if (q.error()) {
      exec.signal(q);
      return null;
    }
    exec.setSqlSuccess();
    ResultSet rs = q.getResultSet();
    if (rs == null) {
      return null;
    }

    String columnType = REGULAR_COLUMN;
    Map<String, List<String>> columnInfo = new HashMap<>();
    columnInfo.put(REGULAR_COLUMN, new ArrayList<>());
    columnInfo.put(PARTITION_COLUMN, new ArrayList<>());
    try {
      while (rs.next()) {
        String colName = rs.getString(1);
        if (colName == null || colName.equals("")) {
          continue;
        }
        if (colName.startsWith("#")) {
          if (colName.startsWith("# Partition")) {
            columnType = PARTITION_COLUMN;
          }
          continue;
        }
        columnInfo.get(columnType).add(colName);
      }
      columnInfo.get(REGULAR_COLUMN).removeAll(columnInfo.get(PARTITION_COLUMN));
      exec.trace(ctx, tableName + " columns: " + StringUtils.join(columnInfo, ", "));
    } catch (SQLException e) {
      columnInfo.clear();
      exec.trace(ctx, e.getMessage());
    }
    exec.closeQuery(q, exec.conf.defaultConnection);
    return columnInfo;
  }

  /**
   *
   */
  public static List<String> buildRowValues(List<String> cols, List<String> identNames, List<String> rawValues) {
    if (cols == null || cols.size() == 0) {
      return rawValues;
    }

    List<String> allValue = new ArrayList<>();
    List<String> idents = new ArrayList<>(identNames);
    List<String> values = new ArrayList<>(rawValues);
    for (String col : cols) {
      int identIdx = 0;
      for (; identIdx < idents.size(); identIdx++) {
        if (col.equalsIgnoreCase(
            StringUtils.strip(idents.get(identIdx), "`'\""))) {
          break;
        }
      }
      if (identIdx < idents.size()) {
        allValue.add(values.get(identIdx));
        values.remove(identIdx);
        idents.remove(identIdx);
      }
      else {
        allValue.add("null");
      }
    }
    return allValue;
  }

}
