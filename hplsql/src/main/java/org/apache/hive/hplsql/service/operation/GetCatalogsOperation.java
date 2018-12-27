package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;

import java.sql.ResultSet;

public class GetCatalogsOperation extends ObtainResultSetOperation {
    public GetCatalogsOperation(HplsqlSession parentSession) {
        super(parentSession, OperationType.GET_CATALOGS);
    }

    @Override
    public void run() throws HplsqlException {
        setState(OperationState.PENDING);
        try {
            ResultSet result = executor.getCatalogs();
            resultSetDecorator = new ResultSetDecorator(result);
        }catch (HplsqlException e){
            setState(OperationState.ERROR);
            throw e;
        }
        setState(OperationState.FINISHED);
    }
}

