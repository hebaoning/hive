package org.apache.hive.hplsql.service.thrift;

import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSetIpAddressProcessor <I extends TCLIService.Iface> extends TCLIService.Processor<TCLIService.Iface>{
    private static final Logger LOGGER = LoggerFactory.getLogger(TSetIpAddressProcessor.class.getName());

    private static final ThreadLocal<String> THREAD_LOCAL_IP_ADDRESS = new ThreadLocal<>();

    private static final ThreadLocal<String> THREAD_LOCAL_USER_NAME = new ThreadLocal<>();

    public TSetIpAddressProcessor(TCLIService.Iface iface) {
        super(iface);
    }

    @Override
    public boolean process(final TProtocol in, final TProtocol out) throws TException {
        setIpAddress(in);
        setUserName(in);
        try {
            return super.process(in, out);
        } finally {
            THREAD_LOCAL_USER_NAME.remove();
            THREAD_LOCAL_IP_ADDRESS.remove();
        }
    }

    private void setUserName(final TProtocol in) {
        TTransport transport = in.getTransport();
        if (transport instanceof TSaslServerTransport) {
            String userName = ((TSaslServerTransport) transport).getSaslServer().getAuthorizationID();
            THREAD_LOCAL_USER_NAME.set(userName);
        }
    }

    protected void setIpAddress(final TProtocol in) {
        TTransport transport = in.getTransport();
        TSocket tSocket = getUnderlyingSocketFromTransport(transport);
        if (tSocket == null) {
            LOGGER.warn("Unknown Transport, cannot determine ipAddress");
        } else {
            THREAD_LOCAL_IP_ADDRESS.set(tSocket.getSocket().getInetAddress().getHostAddress());
        }
    }

    private TSocket getUnderlyingSocketFromTransport(TTransport transport) {
        while (transport != null) {
            if (transport instanceof TSaslServerTransport) {
                transport = ((TSaslServerTransport) transport).getUnderlyingTransport();
            }
            if (transport instanceof TSaslClientTransport) {
                transport = ((TSaslClientTransport) transport).getUnderlyingTransport();
            }
            if (transport instanceof TSocket) {
                return (TSocket) transport;
            }
        }
        return null;
    }

    public static String getUserIpAddress() {
        return THREAD_LOCAL_IP_ADDRESS.get();
    }

    public static String getUserName() {
        return THREAD_LOCAL_USER_NAME.get();
    }

}
