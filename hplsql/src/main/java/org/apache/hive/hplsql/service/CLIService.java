package org.apache.hive.hplsql.service;

import org.apache.hive.hplsql.service.common.conf.ServerConf;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.handle.OperationHandle;
import org.apache.hive.hplsql.service.common.handle.SessionHandle;
import org.apache.hive.hplsql.service.operation.Operation;
import org.apache.hive.hplsql.service.operation.OperationStatus;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.hplsql.service.session.SessionManager;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.rpc.thrift.TGetInfoType;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CLIService {
    private final Logger LOG = LoggerFactory.getLogger(CLIService.class.getName());
    public static final TProtocolVersion SERVER_VERSION;
    private SessionManager sessionManager = new SessionManager();
    private ServerConf serverConf = new ServerConf();
    static {
        TProtocolVersion[] protocols = TProtocolVersion.values();
        SERVER_VERSION = protocols[protocols.length - 1];
    }

    public void init() {
        serverConf.init();
    }

    public SessionHandle openSession(TProtocolVersion protocol, String username, String password, String ipAddress) throws HplsqlException {
        SessionHandle sessionHandle = sessionManager.openSession(protocol, username, password, ipAddress, serverConf);
        LOG.debug(sessionHandle + ": openSession()");
        return sessionHandle;
    }

    public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
                                                 Map<String, String> confOverlay) throws HplsqlException {
        HplsqlSession session = sessionManager.getSession(sessionHandle);
        OperationHandle opHandle = session.executeStatementAsync(statement, confOverlay);
        LOG.debug(sessionHandle + ": executeStatementAsync()");
        return opHandle;
    }

    public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
                                            Map<String, String> confOverlay) throws HplsqlException {
        HplsqlSession session = sessionManager.getSession(sessionHandle);
        OperationHandle opHandle = session.executeStatement(statement, confOverlay);
        LOG.debug(sessionHandle + ": executeStatement()");
        return opHandle;
    }

    public OperationHandle getTypeInfo(SessionHandle sessionHandle)
            throws HplsqlException {
        OperationHandle opHandle = sessionManager.getSession(sessionHandle).getTypeInfo();
        LOG.debug(sessionHandle + ": getTypeInfo()");
        return opHandle;
    }

    public OperationHandle getCatalogs(SessionHandle sessionHandle) throws HplsqlException {
        OperationHandle opHandle = sessionManager.getSession(sessionHandle).getCatalogs();
        LOG.debug(sessionHandle + ": getCatalogs()");
        return opHandle;
    }

    public OperationHandle getSchemas(SessionHandle sessionHandle)
            throws HplsqlException {
        OperationHandle opHandle = sessionManager.getSession(sessionHandle).getSchemas();
        LOG.debug(sessionHandle + ": getSchemas()");
        return opHandle;
    }

    public OperationHandle getTables(SessionHandle sessionHandle, String catalogName,
                                     String schemaName, String tableName, List<String> tableTypes) throws HplsqlException {
        OperationHandle opHandle = sessionManager.getSession(sessionHandle).getTables(catalogName, schemaName, tableName, tableTypes);
        LOG.debug(sessionHandle + ": getTables()");
        return opHandle;
    }

    public OperationHandle getColumns(SessionHandle sessionHandle, String catalog, String schemaPattern,
                                      String tableNamePattern, String columnNamePattern) throws HplsqlException {
        OperationHandle opHandle = sessionManager.getSession(sessionHandle).getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        LOG.debug(sessionHandle + ": getColumns()");
        return opHandle;
    }

    public OperationHandle getFunctions(SessionHandle sessionHandle, String catalog,
                                        String schemaPattern, String functionNamePattern) throws HplsqlException {
        OperationHandle opHandle = sessionManager.getSession(sessionHandle).getFunctions(catalog, schemaPattern, functionNamePattern);
        LOG.debug(sessionHandle + ": getFunctions()");
        return opHandle;
    }

    public OperationStatus getOperationStatus(OperationHandle opHandle)
            throws HplsqlException {
        Operation operation = sessionManager.getOperationManager().getOperation(opHandle);
        if (operation.shouldRunAsync()) {
            try {
                long timeout = ServerConf.OPERATION_STATUS_POLLING_TIMEOUT;
                operation.getBackgroundHandle().get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.trace(opHandle + ": Long polling timed out");
            } catch (CancellationException e) {
                LOG.trace(opHandle + ": The background operation was cancelled", e);
            } catch (ExecutionException e) {
                LOG.warn(opHandle + ": The background operation was aborted", e);
            } catch (InterruptedException e) {
                // In this case, the call might return sooner than long polling timeout
            }
        }
        OperationStatus opStatus = operation.getStatus();
        LOG.debug(opHandle + ": getOperationStatus()");
        return opStatus;
    }

    public TableSchema getResultSetMetadata(OperationHandle opHandle)
            throws HplsqlException {
        TableSchema tableSchema = sessionManager.getOperationManager()
                .getOperation(opHandle).getParentSession().getResultSetMetadata(opHandle);
        LOG.debug(opHandle + ": getResultSetMetadata()");
        return tableSchema;
    }

    public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
                               long maxRows, FetchType fetchType) throws HplsqlException {
        RowSet rowSet = sessionManager.getOperationManager().getOperation(opHandle)
                .getParentSession().fetchResults(opHandle, orientation, maxRows, fetchType);
        LOG.debug(opHandle + ": fetchResults()");
        return rowSet;
    }

    public String getInfo(SessionHandle sessionHandle, TGetInfoType getInfoType) throws HplsqlException {
        String infoValue = sessionManager.getSession(sessionHandle)
                .getInfo(getInfoType);
        LOG.debug(sessionHandle + ": getInfo()");
        return infoValue;
    }

    public void closeOperation(OperationHandle operationHandle) throws HplsqlException {
        Operation operation;
        try {
            operation = sessionManager.getOperationManager().getOperation(operationHandle);
        }catch (HplsqlException e){
            LOG.info(operationHandle + ": closeOperation() failed, the operation is not existed");
            return;
        }
        operation.getParentSession().closeOperation(operationHandle);
        LOG.debug(operationHandle + ": closeOperation()");
    }

    public void cancelOperation(OperationHandle operationHandle) throws HplsqlException {
        Operation operation;
        try {
            operation = sessionManager.getOperationManager().getOperation(operationHandle);
        }catch (HplsqlException e){
            LOG.info(operationHandle + ": cancelOperation() failed, the operation is not existed");
            return;
        }
        operation.getParentSession().cancelOperation(operationHandle);
        LOG.debug(operationHandle + ": cancelOperation()");
    }

    public void closeSession(SessionHandle sessionHandle) throws HplsqlException {
        sessionManager.closeSession(sessionHandle);
        LOG.debug(sessionHandle + ": closeSession()");
    }
}
