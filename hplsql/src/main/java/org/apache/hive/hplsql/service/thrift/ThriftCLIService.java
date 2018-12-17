package org.apache.hive.hplsql.service.thrift;

import org.apache.hive.hplsql.service.CLIService;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.handle.OperationHandle;
import org.apache.hive.hplsql.service.common.handle.SessionHandle;
import org.apache.hive.hplsql.service.operation.OperationStatus;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class ThriftCLIService implements TCLIService.Iface, Runnable {

    public static final Logger LOG = LoggerFactory.getLogger(ThriftCLIService.class.getName());

    protected CLIService cliService;

    private static final TStatus OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS);

    public ThriftCLIService(CLIService service) {
        this.cliService = service;
    }

    public void init() {
        cliService.init();
    }

    @Override
    public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
        LOG.info("-------openSession---------");
        TOpenSessionResp resp = new TOpenSessionResp();
        try {
            SessionHandle sessionHandle = getSessionHandle(req, resp);
            resp.setSessionHandle(sessionHandle.toTSessionHandle());
            resp.setStatus(OK_STATUS);
        } catch (Exception e) {
            LOG.warn("Error opening session: ", e);
            resp.setStatus(HplsqlException.toTStatus(e));
        }
        return resp;
    }

    SessionHandle getSessionHandle(TOpenSessionReq req, TOpenSessionResp res) throws HplsqlException {
        String userName = getUserName(req);
        String ipAddress = getIpAddress();
        TProtocolVersion protocol = getMinVersion(CLIService.SERVER_VERSION,
                req.getClient_protocol());
        SessionHandle sessionHandle = cliService.openSession(protocol, userName, req.getPassword(),
                ipAddress);
        res.setServerProtocolVersion(protocol);
        return sessionHandle;
    }

    /**
     * 只处理了NOSASL类型
     *
     * @param req
     * @return
     */
    private String getUserName(TOpenSessionReq req){
        String userName = null;
        // NOSASL
        if (userName == null) {
            userName = TSetIpAddressProcessor.getUserName();
        }
        if (userName == null) {
            userName = req.getUsername();
        }
        LOG.debug("Client's username: " + userName);
        return userName;
    }

    /**
     * 只处理了NOSASL类型
     *
     * @return
     */
    private String getIpAddress() {
        String clientIpAddress;
        // NOSASL
        clientIpAddress = TSetIpAddressProcessor.getUserIpAddress();
        LOG.debug("Client's IP Address: " + clientIpAddress);
        return clientIpAddress;
    }

    private TProtocolVersion getMinVersion(TProtocolVersion... versions) {
        TProtocolVersion[] values = TProtocolVersion.values();
        int current = values[values.length - 1].getValue();
        for (TProtocolVersion version : versions) {
            if (current > version.getValue()) {
                current = version.getValue();
            }
        }
        for (TProtocolVersion version : values) {
            if (version.getValue() == current) {
                return version;
            }
        }
        throw new IllegalArgumentException("never");
    }

    @Override
    public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
        LOG.info("-------CloseSession---------");
        TCloseSessionResp resp = new TCloseSessionResp();
        try {
            SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
            cliService.closeSession(sessionHandle);
            resp.setStatus(OK_STATUS);
        } catch (Exception e) {
            LOG.warn("Error closing session: ", e);
            resp.setStatus(HiveSQLException.toTStatus(e));
        }
        return resp;
    }

    /**
     * 根据GetInfoType获取相应的信息，如DBMS的名称/版本等
     * jdbc调用入口getDatabaseProductName/getDatabaseProductVersion/getSQLKeywords
     *
     * @param req -sessionHandle infoType
     * @return
     * @throws TException
     */
    @Override
    public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
        LOG.info("-------GetInfo--------- type:" + req.getInfoType());
        TGetInfoResp resp = new TGetInfoResp();
        try {
            String info = cliService.getInfo(new SessionHandle(req.getSessionHandle()),
                            req.getInfoType());
            TGetInfoValue value = new TGetInfoValue();
            value.setStringValue(info);
            resp.setInfoValue(value);
            resp.setStatus(OK_STATUS);
            LOG.info("-------GetInfo--------- value:" + info +",type:"+ req.getInfoType());
        } catch (Exception e) {
            LOG.warn("Error getting info: ", e);
            resp.setStatus(HplsqlException.toTStatus(e));
        }
        return resp;
    }

    @Override
    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
        LOG.info("-------ExecuteStatement---------");
        TExecuteStatementResp resp = new TExecuteStatementResp();
        try {
            SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
            String statement = req.getStatement();
            LOG.debug("ReqStatement:" + req.getStatement());
            Map<String, String> confOverlay = req.getConfOverlay();
            Boolean runAsync = req.isRunAsync();
            OperationHandle operationHandle =
                    runAsync ? cliService.executeStatementAsync(sessionHandle, statement, confOverlay)
                            : cliService.executeStatement(sessionHandle, statement, confOverlay);
            resp.setOperationHandle(operationHandle.toTOperationHandle());
            resp.setStatus(OK_STATUS);
        } catch (Exception e) {
            LOG.warn("Error executing statement: ", e);
            resp.setStatus(HplsqlException.toTStatus(e));
        }
        return resp;
    }

    /**
     * 创建一个GetTypeInfoOperation对象，执行获取数据类型（BOOLEAN/TINYINT..）相关信息的操作，并返回opthandle
     * <p>
     * jdbc调用入口HiveDatabaseMetaData.getTypeInfo()
     *
     * @param req -sessionHandle
     * @return
     * @throws TException
     */
    @Override
    public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
        LOG.info("-------GetTypeInfo--------- " );
        TGetTypeInfoResp resp = new TGetTypeInfoResp();
        try {
            OperationHandle operationHandle = cliService.getTypeInfo(new SessionHandle(req.getSessionHandle()));
            resp.setOperationHandle(operationHandle.toTOperationHandle());
            resp.setStatus(OK_STATUS);
        } catch (Exception e) {
            LOG.warn("Error getting type info: ", e);
            resp.setStatus(HplsqlException.toTStatus(e));
        }
        return resp;
    }


    /**
     * 根据sessionhandle，找到session，调用session的获取目录方法
     * jdbc调用入口HiveDatabaseMetaData.getCatalogs()
     *
     * @param tGetCatalogsReq -sessionHandle
     * @return
     * @throws TException
     */
    @Override
    public TGetCatalogsResp GetCatalogs(TGetCatalogsReq tGetCatalogsReq) throws TException {
        LOG.info("-------GetCatalogs---------");
        return null;
    }


    /**
     * 获取数据库信息
     * jdbc调用入口HiveDatabaseMetaData.getSchemas(String catalog, String schemaPattern)
     *
     * @param tGetSchemasReq -sessionHandle catalogName schemaName
     * @return
     * @throws TException
     */
    @Override
    public TGetSchemasResp GetSchemas(TGetSchemasReq tGetSchemasReq) throws TException {
        LOG.info("-------GetSchemas---------");
        return null;
    }

    /**
     * 获取表信息
     * jdbc调用入口HiveDatabaseMetaData.getTables(String catalog, String schemaPattern,
     * String tableNamePattern, String[] types)
     *
     * @param tGetTablesReq -sessionHandle catalogName schemaName tableName tableTypes
     * @return
     * @throws TException
     */
    @Override
    public TGetTablesResp GetTables(TGetTablesReq tGetTablesReq) throws TException {
        LOG.info("-------GetTables---------");
        return null;
    }

    /**
     * jdbc调用入口HiveDatabaseMetaData.getTableTypes()
     *
     * @param tGetTableTypesReq -sessionHandle
     * @return
     * @throws TException
     */
    @Override
    public TGetTableTypesResp GetTableTypes(TGetTableTypesReq tGetTableTypesReq) throws TException {
        LOG.info("-------GetTableTypes---------");
        return null;
    }

    /**
     * jdbc调用入口HiveDatabaseMetaData.getColumns(String catalog, String schemaPattern,
     * String tableNamePattern, String columnNamePattern)
     *
     * @param tGetColumnsReq -sessionHandle catalogName schemaName tableName columnName
     * @return
     * @throws TException
     */
    @Override
    public TGetColumnsResp GetColumns(TGetColumnsReq tGetColumnsReq) throws TException {
        LOG.info("-------GetColumns---------");
        return null;
    }

    /**
     * 获取函数
     * jdbc调用入口HiveDatabaseMetaData.getFunctions(String catalogName, String schemaPattern, String functionNamePattern)
     *
     * @param tGetFunctionsReq -sessionHandle catalogName schemaName functionName
     * @return
     * @throws TException
     */
    @Override
    public TGetFunctionsResp GetFunctions(TGetFunctionsReq tGetFunctionsReq) throws TException {
        LOG.info("-------GetFunctions---------");
        return null;
    }

    /**
     * 获取主键
     * jdbc调用入口HiveDatabaseMetaData.GetPrimaryKeys
     *
     * @param tGetPrimaryKeysReq
     * @return
     * @throws TException
     */
    @Override
    public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq tGetPrimaryKeysReq) throws TException {
        LOG.info("-------GetPrimaryKeys---------");
        return null;
    }

    /**
     * 获取表的关联关系
     * jdbc调用入口HiveDatabaseMetaData.getCrossReference
     *
     * @param tGetCrossReferenceReq
     * @return
     * @throws TException
     */
    @Override
    public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq tGetCrossReferenceReq) throws TException {
        LOG.info("-------GetCrossReference---------");
        return null;
    }

    /**
     * 根据operationHandle查找对应operation的状态，从而得知statement的执行情况
     *
     * @param req
     * @return
     * @throws TException
     */
    @Override
    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
        LOG.info("-------GetOperationStatus---------");
        TGetOperationStatusResp resp = new TGetOperationStatusResp();
        OperationHandle operationHandle = new OperationHandle(req.getOperationHandle());
        try {
            OperationStatus operationStatus = cliService.getOperationStatus(operationHandle);
            resp.setOperationState(operationStatus.getState().toTOperationState());
            resp.setErrorMessage(operationStatus.getState().getErrorMessage());
            HplsqlException opException = operationStatus.getOperationException();
            if (opException != null) {
                resp.setSqlState(opException.getSQLState());
                resp.setErrorCode(opException.getErrorCode());
                resp.setErrorMessage(opException.getMessage());
            }
            resp.setStatus(OK_STATUS);
        } catch (Exception e) {
            LOG.warn("Error getting operation status: ", e);
            resp.setStatus(HplsqlException.toTStatus(e));
        }
        return resp;
    }

    /**
     * 取消操作
     * jdbc调用入口HiveStatement.cancel()
     *
     * @param req
     * @return
     * @throws TException
     */
    @Override
    public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
        LOG.info("-------CancelOperation---------");
        TCancelOperationResp resp = new TCancelOperationResp();
        try {
            //TODO 调用该方法后未能取消SqlOperation，执行语句的线程仍继续运行，待处理。
            cliService.cancelOperation(new OperationHandle(req.getOperationHandle()));
            resp.setStatus(OK_STATUS);
        } catch (Exception e) {
            LOG.warn("Error cancelling operation: ", e);
            resp.setStatus(HiveSQLException.toTStatus(e));
        }
        return resp;
    }

    /**
     * 关闭操作
     * jdbc调用入口：HiveQueryResultSet.close()->HiveStatement.closeOperationHandle(TOperationHandle stmtHandle)
     * HiveStatement.close()
     *
     * @param req
     * @return
     * @throws TException
     */
    @Override
    public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
        LOG.info("-------CloseOperation---------");
        TCloseOperationResp resp = new TCloseOperationResp();
        try {
            cliService.closeOperation(new OperationHandle(req.getOperationHandle()));
            resp.setStatus(OK_STATUS);
        }catch (Exception e){
            LOG.warn("Error close operation : ", e);
            resp.setStatus(HplsqlException.toTStatus(e));
        }
        return resp;
    }

    /**
     * 获取结果集元数据，hive中最终调用driver.getSchema()方法获取resultSchema，包含字段名，字段类型等
     * jdbc调用入口：HiveQueryResultSet构造函数中调用retrieveSchema()
     *
     * @param req -operationHandle
     * @return
     * @throws TException
     */
    @Override
    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req) throws
            TException {
        LOG.info("-------GetResultSetMetadata---------");
        TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp();
        try {
            TableSchema schema = cliService.getResultSetMetadata(new OperationHandle(req.getOperationHandle()));
            resp.setSchema(schema.toTTableSchema());
            resp.setStatus(OK_STATUS);
        } catch (Exception e) {
            LOG.warn("Error getting result set metadata: ", e);
            resp.setStatus(HplsqlException.toTStatus(e));
        }
        return resp;
    }

    /**
     * 获取执行结果集
     * <p>
     * jdbc调用入口：HiveQueryResultSet.next()
     *
     * @param req
     * @return
     * @throws TException -operationHandle orientation maxRows
     */
    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
        LOG.info("-------FetchResults--------- operationHandle:" + req.getOperationHandle());
        TFetchResultsResp resp = new TFetchResultsResp();
        try {
            RowSet rowSet = cliService.fetchResults(
                    new OperationHandle(req.getOperationHandle()),
                    FetchOrientation.getFetchOrientation(req.getOrientation()),
                    req.getMaxRows(),FetchType.getFetchType(req.getFetchType()));
            resp.setResults(rowSet.toTRowSet());
            resp.setHasMoreRows(false);
            resp.setStatus(OK_STATUS);
        } catch (Exception e) {
            LOG.warn("Error fetching results: ", e);
            resp.setStatus(HplsqlException.toTStatus(e));
        }
        return resp;
    }

    /**
     * 获取Token (hive 认证相关)
     * jdbc调用入口 HiveConnection.getDelegationToken
     *
     * @param tGetDelegationTokenReq
     * @return
     * @throws TException
     */
    public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq tGetDelegationTokenReq) throws
            TException {
        LOG.info("-------GetDelegationToken---------");
        return null;
    }

    /**
     * jdbc调用入口 HiveConnection.cancelDelegationToken
     *
     * @param tCancelDelegationTokenReq
     * @return
     * @throws TException
     */
    @Override
    public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq tCancelDelegationTokenReq) throws
            TException {
        LOG.info("-------CancelDelegationToken---------");
        return null;
    }

    /**
     * jdbc调用入口 HiveConnection.renewDelegationToken
     *
     * @param tRenewDelegationTokenReq
     * @return
     * @throws TException
     */
    @Override
    public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq tRenewDelegationTokenReq) throws
            TException {
        LOG.info("-------RenewDelegationToken---------");
        return null;
    }


    /**
     * 获取queryId ，hive中根据session的HiveConf查找queryId
     * jdbc调用入口：HiveStatement.getQueryId()
     *
     * @param tGetQueryIdReq -operationHandle
     * @return
     * @throws TException
     */
    @Override
    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        LOG.info("-------GetQueryId---------");
        return null;
    }


    /**
     * 发送客户端信息
     *
     * @param tSetClientInfoReq
     * @return
     * @throws TException
     */
    @Override
    public TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
        LOG.info("-------SetClientInfo---------");
        return null;
    }
}
