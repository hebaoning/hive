package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;

import java.sql.ResultSet;

public class GetTypeInfoOperation extends ObtainResultSetOperation {

    public GetTypeInfoOperation(HplsqlSession parentSession) {
        super(parentSession, OperationType.GET_TYPE_INFO);
    }

    @Override
    public void run() throws HplsqlException {
        setState(OperationState.PENDING);
        try {
            ResultSet result = executor.getTypeInfo();
            resultSetDecorator = new ResultSetDecorator(result);
        } catch (HplsqlException e) {
            setState(OperationState.ERROR);
            throw e;
        }
        setState(OperationState.FINISHED);
    }
}
