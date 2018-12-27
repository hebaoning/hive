package org.apache.hive.hplsql.service;

import org.apache.hive.hplsql.service.thrift.ThriftBinaryCLIService;
import org.apache.hive.hplsql.service.thrift.ThriftCLIService;

public class Test {
    public static void main(String[] args) {
        System.setProperty("log4j.configurationFile", "hive-log4j2.properties");
        CLIService cliService = new CLIService();
        ThriftCLIService service = new ThriftBinaryCLIService(cliService);
        service.init();
        service.run();
    }
}
