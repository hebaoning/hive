package org.apache.hive.hplsql.service.common.handle;

import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TSessionHandle;

public class SessionHandle extends Handle {

    private final TProtocolVersion protocol;

    public SessionHandle(TProtocolVersion protocol) {
        this.protocol = protocol;
    }

    public SessionHandle(TSessionHandle tSessionHandle) {
        this(tSessionHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
    }

    public SessionHandle(TSessionHandle tSessionHandle, TProtocolVersion protocol) {
        super(tSessionHandle.getSessionId());
        this.protocol = protocol;
    }

    public TSessionHandle toTSessionHandle() {
        TSessionHandle tSessionHandle = new TSessionHandle();
        tSessionHandle.setSessionId(getHandleIdentifier().toTHandleIdentifier());
        return tSessionHandle;
    }

    public TProtocolVersion getProtocolVersion() {
        return protocol;
    }

    @Override
    public String toString() {
        return "SessionHandle [" + getHandleIdentifier() + "]";
    }
}
