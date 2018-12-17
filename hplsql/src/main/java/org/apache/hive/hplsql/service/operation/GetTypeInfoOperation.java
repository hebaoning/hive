package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;

import java.sql.ResultSet;

public class GetTypeInfoOperation extends Operation {

    Executor executor;

    /**
     * 通过jdbc访问hive获取的结果集
     */
    ResultSet result;

    public GetTypeInfoOperation(HplsqlSession parentSession) {
        super(parentSession, OperationType.GET_TYPE_INFO);
        this.executor = parentSession.getExcutor();
    }

    @Override
    public void run() throws HplsqlException {
        result = executor.getTypeInfo();
    }

    @Override
    public TableSchema getResultSetSchema() throws HplsqlException {
        return null;
    }

    @Override
    public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HplsqlException {
        return null;
    }

    @Override
    public void cancel() throws HplsqlException {

    }

    @Override
    public void close() throws HplsqlException {

    }
}
