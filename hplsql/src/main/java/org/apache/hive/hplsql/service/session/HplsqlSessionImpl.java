package org.apache.hive.hplsql.service.session;

import org.apache.hive.hplsql.service.common.conf.ServerConf;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.handle.OperationHandle;
import org.apache.hive.hplsql.service.common.handle.SessionHandle;
import org.apache.hive.hplsql.service.operation.*;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.TGetInfoType;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HplsqlSessionImpl implements HplsqlSession {
    private static final Logger LOG = LoggerFactory.getLogger(HplsqlSessionImpl.class);
    private final SessionHandle sessionHandle;
    private String username;
    private final String password;
    private String ipAddress;
    private SessionManager sessionManager;
    private OperationManager operationManager;

    private Executor executor;
    // Synchronized by locking on itself. 目前同一个session对象不会被多个线程同时使用。
    private final Set<OperationHandle> opHandleSet = new HashSet<>();

    private final long creationTime;
    private volatile long lastAccessTime = System.currentTimeMillis();

    public HplsqlSessionImpl(SessionHandle sessionHandle, TProtocolVersion protocol,
                             String username, String password, String ipAddress) {
        this.username = username;
        this.password = password;
        this.ipAddress = ipAddress;
        creationTime = System.currentTimeMillis();
        this.sessionHandle = sessionHandle != null ? sessionHandle : new SessionHandle(protocol);
    }

    @Override
    public void open(ServerConf serverConf) throws Exception {
        executor = new Executor(serverConf);
        executor.init();
        lastAccessTime = System.currentTimeMillis();
    }

    @Override
    public void close() throws HplsqlException {
        List<OperationHandle> ops;
        synchronized (opHandleSet) {
            ops = new ArrayList<>(opHandleSet);
            opHandleSet.clear();
        }
        for (OperationHandle opHandle : ops) {
            operationManager.closeOperation(opHandle);
        }
        executor.close();
        lastAccessTime = System.currentTimeMillis();
    }

    @Override
    public String getInfo(TGetInfoType infoType) throws HplsqlException {
        String info = executor.getInfo(infoType);
        lastAccessTime = System.currentTimeMillis();
        return info;
    }

    @Override
    public OperationHandle executeStatement(String statement, Map<String, String> confOverlay) throws HplsqlException {
        OperationHandle operationHandle = executeStatementInternal(statement, confOverlay, false);
        return operationHandle;
    }

    @Override
    public OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay) throws HplsqlException {
        OperationHandle operationHandle = executeStatementInternal(statement, confOverlay, true);
        return operationHandle;
    }

    @Override
    public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HplsqlException {
        TableSchema tableSchema = sessionManager.getOperationManager().getOperationResultSetSchema(opHandle);
        lastAccessTime = System.currentTimeMillis();
        return tableSchema;
    }

    private OperationHandle executeStatementInternal(String statement,
                                                     Map<String, String> confOverlay, boolean runAsync) throws HplsqlException {
        ExecuteStatementOperation operation = operationManager.newExecuteStatementOperation(getSession(), statement, confOverlay, runAsync);
        saveAndRunOperation(operation);
        return operation.getHandle();
    }

    @Override
    public OperationHandle getTypeInfo() throws HplsqlException {
        GetTypeInfoOperation operation = operationManager.newGetTypeInfoOperation(getSession());
        saveAndRunOperation(operation);
        return operation.getHandle();
    }

    @Override
    public OperationHandle getCatalogs() throws HplsqlException {
        GetCatalogsOperation operation = operationManager.newGetCatalogsOperation(getSession());
        saveAndRunOperation(operation);
        return operation.getHandle();
    }

    @Override
    public OperationHandle getSchemas() throws HplsqlException {
        GetSchemasOperation operation = operationManager.newGetSchemasOperation(getSession());
        saveAndRunOperation(operation);
        return operation.getHandle();
    }

    @Override
    public OperationHandle getTables(String catalogName, String schemaName, String tableName,
                                     List<String> tableTypes) throws HplsqlException {
        GetTablesOperation operation = operationManager.newGetTablesOperation(getSession(), catalogName, schemaName, tableName, tableTypes);
        saveAndRunOperation(operation);
        return operation.getHandle();
    }

    @Override
    public OperationHandle getColumns(String catalog, String schemaPattern,
                                      String tableNamePattern, String columnNamePattern) throws HplsqlException {
        GetColumnsOperation operation =
                operationManager.newGetColumnsOperation(getSession(), catalog, schemaPattern, tableNamePattern, columnNamePattern);
        saveAndRunOperation(operation);
        return operation.getHandle();
    }

    @Override
    public OperationHandle getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws HplsqlException {
        GetFunctionsOperarion operation =
                operationManager.newGetFunctionsOperarion(getSession(), catalog, schemaPattern, functionNamePattern);
        saveAndRunOperation(operation);
        return operation.getHandle();
    }



    /**
     * 保存operationHandle，并执行operation
     * @param operation
     * @throws HplsqlException
     */
    private void saveAndRunOperation(Operation operation) throws HplsqlException{
        if(operation != null){
            OperationHandle opHandle = operation.getHandle();
            try {
                addOpHandle(operation.getHandle());
                operation.run();
                lastAccessTime = System.currentTimeMillis();
            } catch (HplsqlException e) {
                if (opHandle != null) {
                    removeOpHandle(opHandle);
                    operationManager.closeOperation(opHandle);
                }
                throw e;
            }
        }
    }

    private void addOpHandle(OperationHandle opHandle) {
        synchronized (opHandleSet) {
            opHandleSet.add(opHandle);
        }
    }

    private void removeOpHandle(OperationHandle opHandle) {
        synchronized (opHandleSet) {
            opHandleSet.remove(opHandle);
        }
    }

    @Override
    public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
                               long maxRows, FetchType fetchType) throws HplsqlException {
        RowSet rowSet;
        if (fetchType == FetchType.QUERY_OUTPUT) {
            rowSet = operationManager.getOperationNextRowSet(opHandle, orientation, maxRows);
        }else{
            //TODO 其他类型的结果集
            rowSet = null;
        }
        lastAccessTime = System.currentTimeMillis();
        return rowSet;
    }

    @Override
    public void closeOperation(OperationHandle operationHandle) throws HplsqlException{
        operationManager.closeOperation(operationHandle);
        synchronized (opHandleSet) {
            opHandleSet.remove(operationHandle);
        }
        lastAccessTime = System.currentTimeMillis();
    }

    @Override
    public void cancelOperation(OperationHandle operationHandle) throws HplsqlException{
        operationManager.cancelOperation(operationHandle);
        lastAccessTime = System.currentTimeMillis();
    }

    @Override
    public String getUserName() {
        return username;
    }

    @Override
    public String getIpAddress() {
        return ipAddress;
    }

    @Override
    public Executor getExcutor() {
        return executor;
    }

    @Override
    public TProtocolVersion getProtocolVersion() {
        return sessionHandle.getProtocolVersion();
    }

    @Override
    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public SessionManager getSessionManager() {
        return sessionManager;
    }

    @Override
    public void setOperationManager(OperationManager operationManager) {
        this.operationManager = operationManager;
    }

    @Override
    public SessionHandle getSessionHandle() {
        return sessionHandle;
    }

    @Override
    public OperationManager getOperationManager() {
        return operationManager;
    }

    protected HplsqlSession getSession() {
        return this;
    }
}
