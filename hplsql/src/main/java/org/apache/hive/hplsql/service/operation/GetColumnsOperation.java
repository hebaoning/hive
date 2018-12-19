package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;

import java.sql.ResultSet;

public class GetColumnsOperation extends GetDatabaseMetaDataOperation{
    String catalog;
    String schemaPattern;
    String tableNamePattern;
    String columnNamePattern;
    public GetColumnsOperation(HplsqlSession parentSession, String catalog, String schemaPattern,
                               String tableNamePattern, String columnNamePattern) {
        super(parentSession, OperationType.GET_COLUMNS);
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.tableNamePattern = tableNamePattern;
        this.columnNamePattern = columnNamePattern;
    }

    @Override
    public void run() throws HplsqlException {
        ResultSet result = executor.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        resultSetDecorator = new ResultSetDecorator(result);
    }
}
