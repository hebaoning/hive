package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;

import java.sql.ResultSet;

public class GetFunctionsOperarion extends ObtainResultSetOperation {
    String catalog;
    String schemaPattern;
    String functionNamePattern;

    public GetFunctionsOperarion(HplsqlSession parentSession, String catalog, String schemaPattern, String functionNamePattern) {
        super(parentSession, OperationType.GET_FUNCTIONS);
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.functionNamePattern = functionNamePattern;
    }

    @Override
    public void run() throws HplsqlException {
        ResultSet result = executor.getFunctions(catalog, schemaPattern, functionNamePattern);
        resultSetDecorator = new ResultSetDecorator(result);
    }
}
