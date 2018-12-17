package org.apache.hive.hplsql.service.operation;


import org.apache.hive.hplsql.service.common.exception.HplsqlException;

/**
 * OperationStatus
 *
 */
public class OperationStatus {

  private final OperationState state;
  private final long operationStarted;
  private final long operationCompleted;
  private final boolean hasResultSet;
  private final HplsqlException operationException;

  public OperationStatus(OperationState state, long operationStarted, long operationCompleted, boolean hasResultSet, HplsqlException operationException) {
    this.state = state;
    this.operationStarted = operationStarted;
    this.operationCompleted = operationCompleted;
    this.hasResultSet = hasResultSet;
    this.operationException = operationException;
  }

  public OperationState getState() {
    return state;
  }

  public long getOperationStarted() {
    return operationStarted;
  }

  public long getOperationCompleted() {
    return operationCompleted;
  }

  public boolean getHasResultSet() {
    return hasResultSet;
  }

  public HplsqlException getOperationException() {
    return operationException;
  }
}
