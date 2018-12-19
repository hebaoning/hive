package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;

public abstract class GetDatabaseMetaDataOperation extends Operation{
    protected Executor executor;

    /**
     * 包装jdbc访问hive获取的结果集
     */
    protected ResultSetDecorator resultSetDecorator;

    protected GetDatabaseMetaDataOperation(HplsqlSession parentSession, OperationType opType) {
        super(parentSession, opType);
        this.executor = getParentSession().getExcutor();
    }

    @Override
    public abstract void run() throws HplsqlException;

    @Override
    public TableSchema getResultSetSchema() throws HplsqlException {
        return resultSetDecorator.getResultSetSchema();
    }

    @Override
    public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HplsqlException {
        try{
            resultSetDecorator.getResult().next();
            RowSet rowSet = resultSetDecorator.getRowSet();
            resultSetDecorator.clearRowSet();
            return rowSet;
        }catch (Exception e){
            throw new HplsqlException("get next rowset failed :" + e.getMessage());
        }
    }

    @Override
    public void cancel() throws HplsqlException {
        try{
            resultSetDecorator.getResult().close();
        }catch (Exception e){
            throw new HplsqlException("close resultset failed :" + e.getMessage());
        }
    }

    @Override
    public void close() throws HplsqlException {
        cancel();
    }
}
