package org.apache.hive.hplsql.service.thrift;

import org.apache.hive.hplsql.service.CLIService;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftBinaryCLIService  extends ThriftCLIService {

    public static final Logger LOG = LoggerFactory.getLogger(ThriftBinaryCLIService.class.getName());

    public static int port;

    public ThriftBinaryCLIService(int port) {
        this.port = port;
    }

    public void run() {
        try {
            // 传输层(Transport), 设置监听端口为9000
            TServerSocket serverSocket = new TServerSocket(port);
            // 协议层
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory(true, true);
            // 处理层(Processor)
            TCLIService.Processor<ThriftBinaryCLIService> processor = new TSetIpAddressProcessor(this);
            // 服务层(Server)
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverSocket)
                            .protocolFactory(protocolFactory)
                            .processor(processor));
            LOG.info("--------start on {} finish---------", port);
            // 启动监听服务
            server.serve();
        }catch (Throwable t) {
            LOG.error("start service fail: " + t.getMessage());
            t.printStackTrace();
        }

    }
}
