package org.apache.hive.hplsql.service.operation;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.jdbc.HiveBaseResultSet;
import org.apache.hive.jdbc.HiveQueryResultSet;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.ResultSet;

public class ResultSetDecorator {
    private final Logger LOG = LoggerFactory.getLogger(ResultSetDecorator.class.getName());
    ResultSet result;

    public ResultSetDecorator(ResultSet result) {
        this.result = result;
    }

    public TableSchema getResultSetSchema() throws HplsqlException{
        TableSchema schema = (TableSchema)getResultSetFieldValue("schema");
        return schema;
    }

    public RowSet getRowSet() throws HplsqlException{
        RowSet rowSet = (RowSet)getResultSetFieldValue("fetchedRows");
        return rowSet;
    }

    public void clearRowSet() throws HplsqlException{
        setResultSetFieldValue("fetchedRows", null);
    }

    private void setResultSetFieldValue(String filedName, Object value) throws HplsqlException{
        try {
            Field field = getResultSetField(filedName);
            if(field != null){
                field.set(result, value);
            }
        }catch (Exception e){
            throw new HplsqlException("get filed("+ filedName +") from resultset obj failed" );
        }
    }

    private Object getResultSetFieldValue(String filedName) throws HplsqlException{
        try {
            Field field = getResultSetField(filedName);
            return field == null ? null : field.get(result);
        }catch (Exception e){
            throw new HplsqlException("get filed("+ filedName +") from resultset obj failed" );
        }
    }

    private Field getResultSetField(String filedName){
        Class<?> baseResultSetClass = HiveBaseResultSet.class;
        Class<?> queryResultSetClass = HiveQueryResultSet.class;
        Field field = getClassField(baseResultSetClass, filedName);
        if(field == null) {
            field = getClassField(queryResultSetClass, filedName);
        }
        return field;
    }

    private Field getClassField(Class obj ,String filedName){
        Field[] f = obj.getDeclaredFields();
        for (Field field : f) {
            if (field.getName().equals(filedName)) {
                field.setAccessible(true);
                return field;
            }
        }
        return null;
    }

    public ResultSet getResult() {
        return result;
    }
}
