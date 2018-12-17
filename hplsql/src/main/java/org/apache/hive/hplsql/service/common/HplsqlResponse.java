package org.apache.hive.hplsql.service.common;

public class HplsqlResponse {
    private final int responseCode;
    private byte[] resultBytes;

    public HplsqlResponse(int responseCode) {
        this.responseCode = responseCode;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public byte[] getResultBytes() {
        return resultBytes;
    }

    public void setResultBytes(byte[] resultBytes) {
        this.resultBytes = resultBytes;
    }
}
