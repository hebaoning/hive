package org.apache.hive.hplsql.service.common.exception;

import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HplsqlException extends SQLException{

    private static final long serialVersionUID = -3885556671935841593L;

    public HplsqlException() {
        super();
    }

    public HplsqlException(String reason) {
        super(reason);
    }

    public HplsqlException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public HplsqlException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }
    /**
     * Converts current object to a {@link TStatus} object
     * @return	a {@link TStatus} object
     */
    public TStatus toTStatus() {
        // TODO: convert sqlState, etc.
        TStatus tStatus = new TStatus(TStatusCode.ERROR_STATUS);
        tStatus.setSqlState(getSQLState());
        tStatus.setErrorCode(getErrorCode());
        tStatus.setErrorMessage(getMessage());
        tStatus.setInfoMessages(toString(this));
        return tStatus;
    }

    /**
     * Converts the specified {@link Exception} object into a {@link TStatus} object
     * @param e	a {@link Exception} object
     * @return	a {@link TStatus} object
     */
    public static TStatus toTStatus(Exception e) {
        if (e instanceof HplsqlException) {
            return ((HplsqlException)e).toTStatus();
        }
        TStatus tStatus = new TStatus(TStatusCode.ERROR_STATUS);
        tStatus.setErrorMessage(e.getMessage());
        tStatus.setInfoMessages(toString(e));
        return tStatus;
    }

    public static List<String> toString(Throwable ex) {
        return toString(ex, null);
    }

    private static List<String> toString(Throwable cause, StackTraceElement[] parent) {
        StackTraceElement[] trace = cause.getStackTrace();
        int m = trace.length - 1;
        if (parent != null) {
            int n = parent.length - 1;
            while (m >= 0 && n >= 0 && trace[m].equals(parent[n])) {
                m--; n--;
            }
        }
        List<String> detail = enroll(cause, trace, m);
        cause = cause.getCause();
        if (cause != null) {
            detail.addAll(toString(cause, trace));
        }
        return detail;
    }

    private static List<String> enroll(Throwable ex, StackTraceElement[] trace, int max) {
        List<String> details = new ArrayList<String>();
        StringBuilder builder = new StringBuilder();
        builder.append('*').append(ex.getClass().getName()).append(':');
        builder.append(ex.getMessage()).append(':');
        builder.append(trace.length).append(':').append(max);
        details.add(builder.toString());
        for (int i = 0; i <= max; i++) {
            builder.setLength(0);
            builder.append(trace[i].getClassName()).append(':');
            builder.append(trace[i].getMethodName()).append(':');
            String fileName = trace[i].getFileName();
            builder.append(fileName == null ? "" : fileName).append(':');
            builder.append(trace[i].getLineNumber());
            details.add(builder.toString());
        }
        return details;
    }


}
