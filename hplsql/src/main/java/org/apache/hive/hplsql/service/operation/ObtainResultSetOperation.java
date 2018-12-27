package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;

/**
 * 获取hive结果集的操作类型
 */
public abstract class ObtainResultSetOperation extends Operation {
    /**
     * 包装jdbc访问hive获取的结果集
     */
    protected ResultSetDecorator resultSetDecorator;

    public ObtainResultSetOperation(HplsqlSession parentSession, OperationType opType) {
        super(parentSession, opType);
    }

    @Override
    public abstract void run() throws HplsqlException;

    @Override
    public TableSchema getResultSetSchema() throws HplsqlException {
        return resultSetDecorator.getResultSetSchema();
    }

    @Override
    public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HplsqlException {
        try {
            resultSetDecorator.getResult().next();
            RowSet rowSet = resultSetDecorator.getRowSet();
            resultSetDecorator.clearRowSet();
            return rowSet;
        } catch (Exception e) {
            throw new HplsqlException("get next rowset failed :" + e.getMessage());
        }
    }

    @Override
    public void cancel() throws HplsqlException {
        clean();
        setState(OperationState.CANCELED);
    }

    @Override
    public void close() throws HplsqlException {
        clean();
        setState(OperationState.CLOSED);

    }

    public void clean() throws HplsqlException {
        try {
            resultSetDecorator.getResult().close();
        } catch (Exception e) {
            throw new HplsqlException("close resultset failed :" + e.getMessage());
        }
    }
}
