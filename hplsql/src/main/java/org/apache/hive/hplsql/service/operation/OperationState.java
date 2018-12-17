package org.apache.hive.hplsql.service.operation;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.service.rpc.thrift.TOperationState;

public enum OperationState {
    INITIALIZED(TOperationState.INITIALIZED_STATE, false),
    RUNNING(TOperationState.RUNNING_STATE, false),
    FINISHED(TOperationState.FINISHED_STATE, true),
    CANCELED(TOperationState.CANCELED_STATE, true),
    CLOSED(TOperationState.CLOSED_STATE, true),
    ERROR(TOperationState.ERROR_STATE, true),
    UNKNOWN(TOperationState.UKNOWN_STATE, false),
    PENDING(TOperationState.PENDING_STATE, false);

    private final TOperationState tOperationState;
    private final boolean terminal;
    private String errorMessage;

    OperationState(TOperationState tOperationState, boolean terminal) {
        this.tOperationState = tOperationState;
        this.terminal = terminal;
    }

    public static void validateTransition(OperationState oldState, OperationState newState)
            throws HplsqlException {
        switch (oldState) {
            case INITIALIZED:
                switch (newState) {
                    case PENDING:
                    case RUNNING:
                    case CANCELED:
                    case CLOSED:
                        return;
                }
                break;
            case PENDING:
                switch (newState) {
                    case RUNNING:
                    case FINISHED:
                    case CANCELED:
                    case ERROR:
                    case CLOSED:
                        return;
                }
                break;
            case RUNNING:
                switch (newState) {
                    case FINISHED:
                    case CANCELED:
                    case ERROR:
                    case CLOSED:
                        return;
                }
                break;
            case FINISHED:
            case CANCELED:
            case ERROR:
                if (OperationState.CLOSED.equals(newState)) {
                    return;
                }
            default:
                // fall-through
        }
        throw new HplsqlException("Illegal Operation state transition " +
                "from " + oldState + " to " + newState);
    }

    public void validateTransition(OperationState newState)
            throws HplsqlException {
        validateTransition(this, newState);
    }

    public boolean isTerminal() {
        return terminal;
    }

    public TOperationState toTOperationState() {
        return tOperationState;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(final String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
