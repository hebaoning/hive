package org.apache.hive.hplsql.service.common.conf;

import org.apache.hive.hplsql.Conf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 配置文件，包含hplsql-site.xml配置信息。（可能同时被多个线程访问）
 */
public class ServerConf{
    /**
     * 异步sql执行操作获取结果的轮询时间(ms)
     * 时间过长会阻碍其他请求获取执行结果
     */
    public static final Long OPERATION_STATUS_POLLING_TIMEOUT = 1000L;
    public static final String DEFAULT_CONN_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private Conf hplsqlConf;
    private final Map<String, ArrayList<String>> connInits = new ConcurrentHashMap<>();
    private final Map<String, String> connStrs = new ConcurrentHashMap<>();

    public void init(){
        hplsqlConf = new Conf();
        hplsqlConf.init();
        initOptions();
    }

    /**
     * 获取hplsql-site.xml相关配置信息
     */
    private void initOptions() {
        if(hplsqlConf == null){
            return;
        }
        Iterator<Map.Entry<String,String>> i = hplsqlConf.iterator();
        while (i.hasNext()) {
            Map.Entry<String,String> item = i.next();
            String key = item.getKey();
            String value = item.getValue();
            if (key == null || value == null || !key.startsWith("hplsql.")) {
                continue;
            }
            else if (key.compareToIgnoreCase(Conf.CONN_DEFAULT) == 0) {
                hplsqlConf.defaultConnection = value;
            }
            else if (key.startsWith("hplsql.conn.init.")) {
                setConnectionInit(key.substring(17), value);
            }
            else if (key.startsWith(Conf.CONN_CONVERT)) {
                hplsqlConf.setConnectionConvert(key.substring(20), value);
            }
            else if (key.startsWith("hplsql.conn.")) {
                connStrs.put(key.substring(12), value);
            }
            else if (key.startsWith("hplsql.")) {
                hplsqlConf.setOption(key, value);
            }
        }
    }

    /**
     * 获取连接的初始化sql语句
     * @param name 连接名称
     * @param connInit 初始化sql语句
     */
    public void setConnectionInit(String name, String connInit) {
        ArrayList<String> a = new ArrayList<>();
        String[] sa = connInit.split(";");
        for (String s : sa) {
            s = s.trim();
            if (!s.isEmpty()) {
                a.add(s);
            }
        }
        connInits.put(name, a);
    }

    public String getConnStrByName(String connName){
        return connStrs.get(connName);
    }

    public String getDefaultConn(){
        if(hplsqlConf == null){
            return null;
        }
        return hplsqlConf.defaultConnection;
    }

    public ArrayList<String> getConnInits(String connName) {
        return connInits.get(connName);
    }
}
