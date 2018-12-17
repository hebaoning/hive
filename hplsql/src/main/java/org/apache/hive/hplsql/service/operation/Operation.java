package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.handle.OperationHandle;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public abstract class Operation {
    public static final Logger LOG = LoggerFactory.getLogger(Operation.class.getName());

    protected final HplsqlSession parentSession;
    private final OperationHandle opHandle;
    private volatile OperationState state = OperationState.INITIALIZED;
    protected boolean hasResultSet;
    protected volatile HplsqlException operationException;
    protected volatile Future<?> backgroundHandle;
    protected OperationResult operationResult;

    private final long beginTime;
    private volatile long lastAccessTime;
    protected long operationStart;
    protected long operationComplete;

    protected Operation(HplsqlSession parentSession, OperationType opType) {
        this(parentSession, null, opType);
    }

    protected Operation(HplsqlSession parentSession, Map<String, String> confOverlay, OperationType opType) {
        this.parentSession = parentSession;
        this.opHandle = new OperationHandle(opType, parentSession.getProtocolVersion());
        beginTime = System.currentTimeMillis();
        lastAccessTime = beginTime;
    }

    public boolean shouldRunAsync() {
        return false; // Most operations cannot run asynchronously.
    }

    public abstract TableSchema getResultSetSchema() throws HplsqlException;

    public abstract RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HplsqlException;

    public abstract void cancel() throws HplsqlException;

    public abstract void close() throws HplsqlException;

    public abstract void run() throws HplsqlException;

    /**
     * 设置operation状态
     * @param newState
     * @return
     * @throws HplsqlException
     */
    protected final OperationState setState(OperationState newState) throws HplsqlException {
        state.validateTransition(newState);
        this.state = newState;
        markTimeOnState(state);
        this.lastAccessTime = System.currentTimeMillis();
        return this.state;
    }


    /**
     * 当前状态为开始或结束状态时，记录时间
     *
     * @param state
     */
    protected void markTimeOnState(OperationState state) {
        switch (state) {
        case RUNNING:
            markOperationStartTime();
            break;
        case ERROR:
        case FINISHED:
        case CANCELED:
            markOperationCompletedTime();
            break;
        }
    }

    protected void markOperationStartTime() {
        operationStart = System.currentTimeMillis();
    }

    protected void markOperationCompletedTime() {
        operationComplete = System.currentTimeMillis();
    }

    /**
     * 设置是否有结果集
     * @param hasResultSet
     */
    protected void setHasResultSet(boolean hasResultSet) {
        this.hasResultSet = hasResultSet;
        opHandle.setHasResultSet(hasResultSet);
    }

    protected void setOperationException(HplsqlException operationException) {
        this.operationException = operationException;
    }

    /**
     * 验证操作对象的当前状态是否符合给定的状态
     * @param states
     * @throws HplsqlException
     */
    protected final void assertState(List<OperationState> states) throws HplsqlException {
        if (!states.contains(state)) {
            throw new HplsqlException("Expected states: " + states.toString() + ", but found "
                    + this.state);
        }
        this.lastAccessTime = System.currentTimeMillis();
    }
    /**
     * 获取Operation状态信息
     * @return
     */
    public OperationStatus getStatus() {
        return new OperationStatus(state,operationStart, operationComplete, hasResultSet,
                operationException);
    }

    public Future<?> getBackgroundHandle() {
        return backgroundHandle;
    }

    protected void setBackgroundHandle(Future<?> backgroundHandle) {
        this.backgroundHandle = backgroundHandle;
    }

    public OperationHandle getHandle() {
        return opHandle;
    }

    public HplsqlSession getParentSession() {
        return parentSession;
    }

    public TProtocolVersion getProtocolVersion() {
        return opHandle.getProtocolVersion();
    }

    public OperationResult getOperationResult() {
        return operationResult;
    }
}
