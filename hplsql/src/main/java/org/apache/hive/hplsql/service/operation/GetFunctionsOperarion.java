package org.apache.hive.hplsql.service.operation;

import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.hplsql.service.common.conf.ServerConf;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.utils.FileUtils;
import org.apache.hive.hplsql.service.session.HplsqlSession;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;

import java.sql.DatabaseMetaData;
import java.util.List;

public class GetFunctionsOperarion extends Operation {
    private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
            .addPrimitiveColumn("FUNCTION_CAT", Type.STRING_TYPE,
                    "Function catalog (may be null)")
            .addPrimitiveColumn("FUNCTION_SCHEM", Type.STRING_TYPE,
                    "Function schema (may be null)")
            .addPrimitiveColumn("FUNCTION_NAME", Type.STRING_TYPE,
                    "Function name. This is the name used to invoke the function")
            .addPrimitiveColumn("REMARKS", Type.STRING_TYPE,
                    "Explanatory comment on the function")
            .addPrimitiveColumn("FUNCTION_TYPE", Type.INT_TYPE,
                    "Kind of function.")
            .addPrimitiveColumn("SPECIFIC_NAME", Type.STRING_TYPE,
                    "The name which uniquely identifies this function within its schema");

    private String catalog;
    private String schemaPattern;
    private String functionNamePattern;
    private OperationResult result;


    public GetFunctionsOperarion(HplsqlSession parentSession, String catalog, String schemaPattern, String functionNamePattern) {
        super(parentSession, OperationType.GET_FUNCTIONS);
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.functionNamePattern = functionNamePattern;
    }

    @Override
    public void run() throws HplsqlException {
        setState(OperationState.PENDING);
        List<String> procedureNames = FileUtils.getFileNames(ServerConf.PROCEDURES_DIR + schemaPattern);
        StringBuilder builder = new StringBuilder();
        for (String name : procedureNames) {
            builder.append(name + "\n");
        }
        result = new OperationResult(builder.toString().getBytes());
        setState(OperationState.FINISHED);
    }

    @Override
    public TableSchema getResultSetSchema() throws HplsqlException {
        return RESULT_SET_SCHEMA;
    }

    @Override
    public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HplsqlException {
        RowSet rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion(),false);
        List<String> procedureNames = result.read(isFetchFirst(orientation), maxRows);
        for(String procedureName : procedureNames) {
            Object[] rowData= new Object[]{
                    ServerConf.PROCEDURES_DIR + schemaPattern, // FUNCTION_CAT
                    schemaPattern, // FUNCTION_SCHEM
                    procedureName, // FUNCTION_NAME
                    "" , // REMARKS
                    DatabaseMetaData.functionNullableUnknown, // FUNCTION_TYPE
                    ""
            };
            rowSet.addRow(rowData);
        }
        return rowSet;
    }

    @Override
    public void cancel() throws HplsqlException {
        result.close(false);
        setState(OperationState.CANCELED);
    }

    @Override
    public void close() throws HplsqlException {
        result.close(false);
        setState(OperationState.CLOSED);
    }

    private boolean isFetchFirst(FetchOrientation fetchOrientation) {
        if (fetchOrientation.equals(FetchOrientation.FETCH_FIRST)) {
            return true;
        }
        return false;
    }
}
