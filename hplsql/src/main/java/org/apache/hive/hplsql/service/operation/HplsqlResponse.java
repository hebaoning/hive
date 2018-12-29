package org.apache.hive.hplsql.service.operation;

import java.io.File;
import java.sql.ResultSet;

public class HplsqlResponse {
    private final int responseCode;
    private File file;
    private byte[] resultBytes;
    private ResultSet resultSet;

    public HplsqlResponse(int responseCode, File file) {
        this.responseCode = responseCode;
        this.file = file;
    }

    public HplsqlResponse(int responseCode, byte[] resultBytes) {
        this.responseCode = responseCode;
        this.resultBytes = resultBytes;
    }

    public HplsqlResponse(int responseCode, ResultSet resultSet) {
        this.responseCode = responseCode;
        this.resultSet = resultSet;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public File getFile() {
        return file;
    }

    public byte[] getResultBytes() {
        return resultBytes;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }
}
