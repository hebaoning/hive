package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;

import java.sql.ResultSet;

public class GetTablesOperation extends ObtainResultSetOperation{
    String catalog;
    String schemaPattern;
    String tableNamePattern;
    String[] types;
    public GetTablesOperation(HplsqlSession parentSession,String catalog, String schemaPattern,
                              String tableNamePattern, String[] types) {
        super(parentSession, OperationType.GET_TABLES);
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.tableNamePattern = tableNamePattern;
        this.types = types;
    }

    @Override
    public void run() throws HplsqlException {
        ResultSet result = executor.getTables(catalog, schemaPattern, tableNamePattern, types);
        resultSetDecorator = new ResultSetDecorator(result);
    }
}
