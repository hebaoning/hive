package org.apache.hive.hplsql.service.operation;

import org.apache.hadoop.io.IOUtils;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class OperationResult {
    private byte[] resultBytes;
    private BufferedReader reader;

    public OperationResult(byte[] resultBytes) {
        this.resultBytes = resultBytes;
    }

    /**
     * 读取结果数据
     * @param isFetchFirst 是否从头取数据
     * @param maxRows 获取数据的最大行数
     * @return
     * @throws HplsqlException
     */
    public synchronized List<String> read(boolean isFetchFirst, long maxRows)
            throws HplsqlException {
        if (isFetchFirst) {
            // reset the BufferReader
            resetReader();
        }
        return readResults(maxRows);
    }

    private void resetReader() {
        if (reader != null) {
            IOUtils.closeStream(reader);
            reader = null;
        }
    }

    private List<String> readResults(long nLines) throws HplsqlException {
        List<String> results = new ArrayList<String>();
        if (reader == null) {
            reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(resultBytes)));
        }
        String line;
        // 如果 nLines <= 0, 读取所有行
        for (int i = 0; i < nLines || nLines <= 0; i++) {
            try {
                line = reader.readLine();
                if (line == null) {
                    break;
                } else {
                    results.add(line);
                }
            } catch (IOException e) {
                throw new HplsqlException("Reading operation result output encountered an exception: ", e);
            }
        }
        return results;
    }

    /**
     * 当关闭操作时，关闭该结果对象
     * @throws HplsqlException
     */
    public synchronized void close() throws HplsqlException{
        try {
            if (reader != null) {
                reader.close();
            }
        }catch(IOException e){
            throw new HplsqlException("closing operation result output encountered an exception: ", e);
        }

    }
}
