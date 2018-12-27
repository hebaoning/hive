package org.apache.hive.hplsql.service.operation;

import org.apache.hive.hplsql.service.common.HplsqlResponse;
import org.apache.hive.hplsql.service.common.conf.ServerConf;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class ExecuteStatementOperation extends ObtainResultSetOperation {
    private static final List<String> specialStatements = new ArrayList<>();
    protected String statement;
    private HplsqlResponse response;
    private TableSchema resultSchema;
    private boolean getResultFromResultSet;
    private final boolean runAsync;
    private boolean isSpecialStmt;
    private OperationResult result;

    static {
        specialStatements.add("SHOW PROCEDURE ");
    }

    public ExecuteStatementOperation(HplsqlSession parentSession, String statement,
                                     Map<String, String> confOverlay, boolean runInBackground) {
        super(parentSession, OperationType.EXECUTE_STATEMENT);
        this.statement = statement;
        this.runAsync = runInBackground;
    }

    public String getStatement() {
        return statement;
    }

    @Override
    public boolean shouldRunAsync() {
        return runAsync;
    }

    @Override
    public void run() throws HplsqlException {
        setState(OperationState.PENDING);
        setHasResultSet(true);
        if (!runAsync) {
            runStatement();
        } else {
            Runnable work = () -> {
                try {
                    runStatement();
                } catch (HplsqlException e) {
                    setOperationException(e);
                    LOG.error("Error running hplsql : ", e);
                }
            };
            Future<?> backgroundHandle = getParentSession().getOperationManager().submitBackgroundOperation(work);
            setBackgroundHandle(backgroundHandle);
        }
    }

    private void runStatement() throws HplsqlException {
        try {
            OperationState opState = getStatus().getState();
            // Operation may have been cancelled by another thread
            if (opState.isTerminal()) {
                LOG.info("Not running the query. Operation is already in terminal state: " + opState);
                return;
            }
            LOG.info(executor + " start execute " + statement);
            int type = typeOfSpecialStmts(statement);
            if (type == -1) {
                runHplsql(statement);
            } else {
                runSpecialStmt(type, statement);
            }
        } catch (Throwable e) {
            if ((getStatus().getState() == OperationState.CANCELED) || (getStatus().getState() == OperationState.CLOSED) || (
                    getStatus().getState() == OperationState.FINISHED)) {
                LOG.warn("Ignore exception in terminal state", e);
                return;
            }
            setState(OperationState.ERROR);
            if (e instanceof HplsqlException) {
                throw (HplsqlException) e;
            } else {
                throw new HplsqlException("Error running statement: " + e.toString(), e);
            }
        }
        setState(OperationState.FINISHED);
    }

    private void runHplsql(String statement) throws Exception {
        response = executor.runHpl(statement, getHandle(), saveResultToFile);
        LOG.info(executor + " execute {} finished", statement);
        if (0 != response.getResponseCode()) {
            throw new HplsqlException("Error while processing statement");
        }
        if (response.getResultSet() != null) {
            getResultFromResultSet = true;
            resultSetDecorator = new ResultSetDecorator(response.getResultSet());
        } else {
            result = saveResultToFile ? new OperationResult(response.getFile()) : new OperationResult(response.getResultBytes());
        }
        result = saveResultToFile ? new OperationResult(response.getFile()) : new OperationResult(response.getResultBytes());

    }

    private int typeOfSpecialStmts(String statement) {
        if (statement == null) {
            return -1;
        }
        String formatStmt = statement.replaceAll(" +", " ").trim();
        if (!formatStmt.contains(";") || formatStmt.indexOf(";") == formatStmt.length() - 1) {
            for (int i = 0; i < specialStatements.size(); i++) {
                if (formatStmt.contains(specialStatements.get(i))) {
                    isSpecialStmt = true;
                    return i;
                }
            }
        }
        return -1;
    }

    private void runSpecialStmt(int type, String stmt) throws HplsqlException {
        switch (type) {
            case 0:
                runShowProcedureStmt(stmt);
                break;
        }
    }

    private void runShowProcedureStmt(String stmt) throws HplsqlException {
        String[] strs = stmt.replaceAll(" +", " ").trim().split(" ");
        if (strs.length > 2) {
            String procedureNameWithSchema = strs[2].contains(";") ? strs[2].substring(0, strs[2].length() - 1) : strs[2];
            String Schema = procedureNameWithSchema.split("\\.")[0];
            String procedureName = procedureNameWithSchema.split("\\.")[1];
            File file = new File(ServerConf.PROCEDURES_DIR
                    + Schema + File.separator + procedureName + ServerConf.PROCEDURES_FILE_EXT);
            if (file.exists()) {
                result = new OperationResult(file);
                return;
            }
            throw new HplsqlException("Couldn't find procedure " + procedureNameWithSchema);
        }
        throw new HplsqlException("not supported statement");
    }

    @Override
    public TableSchema getResultSetSchema() throws HplsqlException {
        if (getResultFromResultSet) {
            return super.getResultSetSchema();
        }
        if (resultSchema == null) {
            resultSchema = new TableSchema().addStringColumn("output", "hpl execute output");
        }
        return resultSchema;
    }

    @Override
    public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HplsqlException {
        if (getResultFromResultSet) {
            return super.getNextRowSet(orientation, maxRows);
        }
        assertState(new ArrayList<>(Arrays.asList(OperationState.FINISHED)));
        TableSchema tableSchema = getTableSchema();
        RowSet rowSet = RowSetFactory.create(tableSchema, getProtocolVersion(), false);

        if (result == null) {
            throw new HplsqlException("Couldn't find operation result: " + getHandle());
        }
        // 读取结果数据
        List<String> resultStrings = result.read(isFetchFirst(orientation), maxRows);

        // 将结果数据转换为RowSet形式返回
        for (String resultString : resultStrings) {
            rowSet.addRow(new String[]{resultString});
        }
        return rowSet;
    }

    private TableSchema getTableSchema() {
        TableSchema schema = new TableSchema();
        schema.addStringColumn("string", "hpl execute result");
        return schema;
    }

    private boolean isFetchFirst(FetchOrientation fetchOrientation) {
        if (fetchOrientation.equals(FetchOrientation.FETCH_FIRST)) {
            return true;
        }
        return false;
    }

    @Override
    public void cancel() throws HplsqlException {
        OperationState opState = getStatus().getState();
        if (opState.isTerminal()) {
            LOG.info("Not cancel the query. Operation is already aborted in state -" + opState);
            return;
        }
        clean();
        setState(OperationState.CANCELED);

    }

    @Override
    public void close() throws HplsqlException {
        clean();
        setState(OperationState.CLOSED);
    }

    @Override
    public void clean() throws HplsqlException {
        cancelExecuteTask();
        if (result != null) {
            result.close(isSpecialStmt ? false : true);
        }
        if (getResultFromResultSet) {
            super.clean();
        }
    }

    private void cancelExecuteTask() {
        if (!shouldRunAsync()) {
            return;
        }
        Future<?> backgroundHandle = getBackgroundHandle();
        if (backgroundHandle != null) {
            boolean success = backgroundHandle.cancel(true);
            if (success) {
                LOG.info(getHandle() + ":The running operation has been successfully interrupted");
            } else {
                LOG.info(getHandle() + "The running operation could not be cancelled, typically because it has already completed normally");
            }
        }
    }

}
