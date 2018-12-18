package org.apache.hive.hplsql.service.common;

import java.io.File;

public class HplsqlResponse {
    private final int responseCode;
    private File file;
    private byte[] resultBytes;

    public HplsqlResponse(int responseCode, File file) {
        this.responseCode = responseCode;
        this.file = file;
    }

    public HplsqlResponse(int responseCode, byte[] resultBytes) {
        this.responseCode = responseCode;
        this.resultBytes = resultBytes;
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

}
