package org.apache.hive.hplsql.service.operation;

import org.apache.hive.service.rpc.thrift.TOperationType;

/**
 * 操作类型
 */
public enum OperationType {
    UNKNOWN_OPERATION(TOperationType.UNKNOWN),
    EXECUTE_STATEMENT(TOperationType.EXECUTE_STATEMENT),
    GET_TYPE_INFO(TOperationType.GET_TYPE_INFO),
    GET_CATALOGS(TOperationType.GET_CATALOGS),
    GET_SCHEMAS(TOperationType.GET_SCHEMAS),
    GET_TABLES(TOperationType.GET_TABLES),
    GET_TABLE_TYPES(TOperationType.GET_TABLE_TYPES),
    GET_COLUMNS(TOperationType.GET_COLUMNS),
    GET_FUNCTIONS(TOperationType.GET_FUNCTIONS);

    private TOperationType tOperationType;

    OperationType(TOperationType tOpType) {
        this.tOperationType = tOpType;
    }

    public static OperationType getOperationType(TOperationType tOperationType) {
        for (OperationType opType : values()) {
            if (tOperationType.equals(opType.tOperationType)) {
                return opType;
            }
        }
        return OperationType.UNKNOWN_OPERATION;
    }

    public TOperationType toTOperationType() {
        return tOperationType;
    }
}
