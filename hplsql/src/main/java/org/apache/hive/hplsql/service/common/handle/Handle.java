package org.apache.hive.hplsql.service.common.handle;

import org.apache.hive.service.rpc.thrift.THandleIdentifier;

public abstract class Handle {
    private final HandleIdentifier handleId;

    public Handle() {
        handleId = new HandleIdentifier();
    }

    public Handle(HandleIdentifier handleId) {
        this.handleId = handleId;
    }

    public Handle(THandleIdentifier tHandleIdentifier) {
        this.handleId = new HandleIdentifier(tHandleIdentifier);
    }

    public HandleIdentifier getHandleIdentifier() {
        return handleId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((handleId == null) ? 0 : handleId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Handle)) {
            return false;
        }
        Handle other = (Handle) obj;
        if (handleId == null) {
            if (other.handleId != null) {
                return false;
            }
        } else if (!handleId.equals(other.handleId)) {
            return false;
        }
        return true;
    }

    @Override
    public abstract String toString();

}
