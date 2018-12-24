package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;

import java.sql.ResultSet;

public class GetSchemasOperation extends ObtainResultSetOperation {

    public GetSchemasOperation(HplsqlSession parentSession) {
        super(parentSession, OperationType.GET_SCHEMAS);
    }

    @Override
    public void run() throws HplsqlException {
        ResultSet result = executor.getSchemas();
        resultSetDecorator = new ResultSetDecorator(result);
    }
}
