package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.handle.OperationHandle;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class OperationManager {
    private final Logger LOG = LoggerFactory.getLogger(OperationManager.class.getName());
    private final ConcurrentHashMap<OperationHandle, Operation> handleToOperation =
            new ConcurrentHashMap<OperationHandle, Operation>();
    private ExecutorService backgroundOperationPool = Executors.newFixedThreadPool(5);

    public ExecuteStatementOperation newExecuteStatementOperation(
            HplsqlSession parentSession, String statement, Map<String, String> confOverlay, boolean runAsync) {
        ExecuteStatementOperation operation =
                new ExecuteStatementOperation(parentSession, statement, confOverlay, runAsync);
        addOperation(operation);
        return operation;
    }

    public GetTypeInfoOperation newGetTypeInfoOperation(HplsqlSession parentSession) {
        GetTypeInfoOperation operation = new GetTypeInfoOperation(parentSession);
        addOperation(operation);
        return operation;
    }

    public GetCatalogsOperation newGetCatalogsOperation(HplsqlSession parentSession) {
        GetCatalogsOperation operation = new GetCatalogsOperation(parentSession);
        addOperation(operation);
        return operation;
    }

    public GetSchemasOperation newGetSchemasOperation(HplsqlSession parentSession) {
        GetSchemasOperation operation = new GetSchemasOperation(parentSession);
        addOperation(operation);
        return operation;
    }

    public GetTablesOperation newGetTablesOperation(HplsqlSession parentSession, String catalogName,
                                                    String schemaName, String tableName, List<String> tableTypes) {
        String[] types = tableTypes == null ? new String[]{} : (String[]) tableTypes.toArray();
        GetTablesOperation operation = new GetTablesOperation(parentSession, catalogName, schemaName, tableName, types);
        addOperation(operation);
        return operation;
    }

    public GetColumnsOperation newGetColumnsOperation(HplsqlSession parentSession, String catalog, String schemaPattern,
                                                  String tableNamePattern, String columnNamePattern) {
        GetColumnsOperation operation = new GetColumnsOperation(
                parentSession, catalog, schemaPattern, tableNamePattern, columnNamePattern);
        addOperation(operation);
        return operation;
    }

    public GetFunctionsOperarion newGetFunctionsOperarion(HplsqlSession parentSession,
                                                          String catalog, String schemaPattern, String functionNamePattern) {
        GetFunctionsOperarion operation = new GetFunctionsOperarion(parentSession, catalog, schemaPattern, functionNamePattern);
        addOperation(operation);
        return operation;
    }

    private void addOperation(Operation operation) {
        LOG.info("Adding operation: " + operation.getHandle());
        handleToOperation.put(operation.getHandle(), operation);
    }

    public TableSchema getOperationResultSetSchema(OperationHandle opHandle)
            throws HplsqlException {
        return getOperation(opHandle).getResultSetSchema();
    }

    public Operation getOperation(OperationHandle operationHandle) throws HplsqlException {
        Operation operation = handleToOperation.get(operationHandle);
        if (operation == null) {
            throw new HplsqlException("Invalid OperationHandle: " + operationHandle);
        }
        return operation;
    }

    public RowSet getOperationNextRowSet(OperationHandle opHandle,
                                         FetchOrientation orientation, long maxRows) throws HplsqlException {
        return getOperation(opHandle).getNextRowSet(orientation, maxRows);
    }

    public Future<?> submitBackgroundOperation(Runnable r) {
        return backgroundOperationPool.submit(r);
    }

    public void closeOperation(OperationHandle opHandle) throws HplsqlException {
        LOG.info("Closing operation: " + opHandle);
        Operation operation = handleToOperation.remove(opHandle);
        if (operation == null) {
            throw new HplsqlException("Operation does not exist: " + opHandle);
        }
        operation.close();
    }

    public void cancelOperation(OperationHandle opHandle) throws HplsqlException{
        LOG.info("Cancel operation: " + opHandle);
        getOperation(opHandle).cancel();
    }
}
