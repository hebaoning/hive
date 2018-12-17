package org.apache.hive.hplsql.service.session;

import org.apache.hive.hplsql.service.common.conf.ServerConf;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.handle.OperationHandle;
import org.apache.hive.hplsql.service.common.handle.SessionHandle;
import org.apache.hive.hplsql.service.operation.Executor;
import org.apache.hive.hplsql.service.operation.OperationManager;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.TGetInfoType;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;

import java.util.Map;

public interface HplsqlSession {

    void open(ServerConf serverConf) throws Exception;

    void close() throws HplsqlException;

    OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay) throws HplsqlException;

    OperationHandle executeStatement(String statement, Map<String, String> confOverlay) throws HplsqlException;

    OperationHandle getTypeInfo() throws HplsqlException;

    String getInfo(TGetInfoType getInfoType) throws HplsqlException;

    TableSchema getResultSetMetadata(OperationHandle opHandle)
            throws HplsqlException;

    RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
                        long maxRows, FetchType fetchType) throws HplsqlException;

    Executor getExcutor();

    TProtocolVersion getProtocolVersion();

    void setSessionManager(SessionManager sessionManager);

    SessionManager getSessionManager();

    void setOperationManager(OperationManager operationManager);

    OperationManager getOperationManager();

    SessionHandle getSessionHandle();

    void closeOperation(OperationHandle operationHandle) throws HplsqlException;

    void cancelOperation(OperationHandle operationHandle) throws HplsqlException;

    String getUserName();

    String getIpAddress();
}
