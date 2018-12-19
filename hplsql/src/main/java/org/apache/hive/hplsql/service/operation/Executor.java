package org.apache.hive.hplsql.service.operation;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.commons.io.FileUtils;
import org.apache.hive.hplsql.Exec;
import org.apache.hive.hplsql.service.common.HplsqlResponse;
import org.apache.hive.hplsql.service.common.conf.ServerConf;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.handle.OperationHandle;
import org.apache.hive.service.rpc.thrift.TGetInfoType;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;

public class Executor {
    private static final Logger LOG = LoggerFactory.getLogger(Executor.class);

    //为了保证一个hplsql会话对应一个hive会话。同一个hplsql session中使用同一个connection连接hive。
    //注：多个线程同时使用同一个connection会报错。
    private Connection reuseConnection;
    private ServerConf serverConf;

    public Executor(ServerConf serverConf) {
        this.serverConf = serverConf;
    }

    public void init() throws HplsqlException {
        reuseConnection = openConnection();
    }

    public synchronized HplsqlResponse runHpl(String statement, OperationHandle operationHandle , boolean saveResultToFile) throws Exception {
        checkConnection();
        OutputStream outputStream;
        File file = null;
        HplsqlResponse response;
        if(saveResultToFile){
            file = new File(Executor.class.getClassLoader().getResource("").getPath()
                    + ServerConf.RESULTS_FILE_DIR + operationHandle.getHandleIdentifier() + ".txt");
            if(file.getParentFile() != null){
                file.getParentFile().mkdirs();
            }
            file.createNewFile();
            outputStream = new FileOutputStream(file);
        } else{
            outputStream = new ByteArrayOutputStream();
        }
        //hplsql执行结果会输出到不同文件，防止不同线程执行时都将结果打印到标准输出，造成结果数据交叉
        PrintWriter printWriter = new PrintWriter(outputStream);
        String[] args = {"-e",  statement};
        int responseCode = excuteHplCmd(args,printWriter);
        printWriter.flush(); //将缓冲区的数据写出到底层输出流中
        response = saveResultToFile ? new HplsqlResponse(responseCode, file)
                : new HplsqlResponse(responseCode, ((ByteArrayOutputStream) outputStream).toByteArray());
        printWriter.close();
        return response;
    }

    private int excuteHplCmd(String[] args, PrintWriter printWriter) throws Exception{
        //hplsql 不一定使用默认连接conf.defaultConnection
        Exec exec = new Exec(reuseConnection, printWriter);
        return exec.run(args);
    }

    //目前不会并发执行该方法，synchronized是保证只有一个线程使用connection对象
    public synchronized String getInfo(TGetInfoType type) throws HplsqlException {
        try {
            checkConnection();
            switch (type) {
                case CLI_DBMS_VER:
                    return reuseConnection.getMetaData().getDatabaseProductVersion();
                case CLI_DBMS_NAME:
                    return reuseConnection.getMetaData().getDatabaseProductName();
                default:
                    throw new HplsqlException("Unrecognized GetInfoType value: " + type.toString());
            }
        } catch (Exception e) {
            throw new HplsqlException("fail to getInfo:" + e.getMessage());
        }
    }

    public synchronized ResultSet getTypeInfo() throws HplsqlException {
        try {
            checkConnection();
            return reuseConnection.getMetaData().getTypeInfo();
        } catch (Exception e) {
            throw new HplsqlException("fail to getTypeInfo:" + e.getMessage());
        }
    }

    public synchronized ResultSet getCatalogs() throws HplsqlException {
        try {
            checkConnection();
            return reuseConnection.getMetaData().getCatalogs();
        } catch (Exception e) {
            throw new HplsqlException("fail to getCatalogs:" + e.getMessage());
        }
    }

    public synchronized ResultSet getSchemas() throws HplsqlException {
        try {
            checkConnection();
            return reuseConnection.getMetaData().getSchemas();
        } catch (Exception e) {
            throw new HplsqlException("fail to getSchemas:" + e.getMessage());
        }
    }

    public synchronized ResultSet getTables(String catalog, String schemaPattern,
                                            String tableNamePattern, String[] types) throws HplsqlException {
        try {
            checkConnection();
            return reuseConnection.getMetaData().getTables(catalog, schemaPattern, tableNamePattern, types);
        } catch (Exception e) {
            throw new HplsqlException("fail to getTables:" + e.getMessage());
        }
    }


    public synchronized ResultSet getColumns(String catalog, String schemaPattern,
                                            String tableNamePattern, String columnNamePattern) throws HplsqlException {
        try {
            checkConnection();
            return reuseConnection.getMetaData().getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        } catch (Exception e) {
            throw new HplsqlException("fail to getColumns:" + e.getMessage());
        }
    }

    public synchronized ResultSet getFunctions(String catalog, String schemaPattern,
                                             String functionNamePattern) throws HplsqlException {
        try {
            checkConnection();
            return reuseConnection.getMetaData().getFunctions(catalog, schemaPattern, functionNamePattern);
        } catch (Exception e) {
            throw new HplsqlException("fail to getFunctions:" + e.getMessage());
        }
    }

    private Connection openConnection() throws HplsqlException {
        try {
            Connection conn = getConnection();
            executeInitSql(conn);
            return conn;
        } catch (Exception e) {
            throw new HplsqlException("open connection fail :" + e.getMessage());
        }
    }

    private Connection getConnection() throws HplsqlException {
        try {
            String driver = ServerConf.DEFAULT_CONN_DRIVER;
            String connStr = serverConf.getConnStrByName(serverConf.getDefaultConn());
            StringBuilder url = new StringBuilder();
            String usr = "";
            String pwd = "";
            if (connStr != null) {
                String[] c = connStr.split(";");
                if (c.length >= 1) {
                    driver = c[0];
                }
                if (c.length >= 2) {
                    url.append(c[1]);
                } else {
                    url.append("jdbc:hive://");
                }
                for (int i = 2; i < c.length; i++) {
                    if (c[i].contains("=")) {
                        url.append(";");
                        url.append(c[i]);
                    } else if (usr.isEmpty()) {
                        usr = c[i];
                    } else if (pwd.isEmpty()) {
                        pwd = c[i];
                    }
                }
            }
            Class.forName(driver);
            return DriverManager.getConnection(url.toString().trim(), usr, pwd);
        } catch (Exception e) {
            throw new HplsqlException("get connection fail :" + e.getMessage());
        }
    }

    private void executeInitSql(Connection conn) throws SQLException {
        ArrayList<String> sqls = serverConf.getConnInits(serverConf.getDefaultConn());     // Run initialization statements on the connection
        if (sqls != null) {
            Statement stm = conn.createStatement();
            for (String sql : sqls) {
                LOG.debug(conn + " execute Init sql :" + sql);
                stm.execute(sql);
            }
            stm.close();
        }
    }

    private void checkConnection() throws SQLException {
        if (reuseConnection.isClosed()) {
            Log.info("{} is closed", reuseConnection);
            reuseConnection = openConnection();
        }
    }

    public void close() throws HplsqlException {
        try {
            reuseConnection.close();
        } catch (Exception e) {
            throw new HplsqlException("close connection fail :" + e.getMessage());
        }
    }
}
