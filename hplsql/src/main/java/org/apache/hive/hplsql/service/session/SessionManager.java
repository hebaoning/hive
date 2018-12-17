package org.apache.hive.hplsql.service.session;
import org.apache.hive.hplsql.service.common.conf.ServerConf;
import org.apache.hive.hplsql.service.common.exception.HplsqlException;
import org.apache.hive.hplsql.service.common.handle.SessionHandle;
import org.apache.hive.hplsql.service.operation.OperationManager;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class SessionManager {
    private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);
    private final Map<SessionHandle, HplsqlSession> handleToSession =
            new ConcurrentHashMap<>();
    private final Map<String, LongAdder> connectionsCount = new ConcurrentHashMap<>();
    private int userLimit;
    private int ipAddressLimit;
    private int userIpAddressLimit;
    private final OperationManager operationManager = new OperationManager();


    /**
     * Opens a new session and creates a session handle.
     * The username passed to this method is the effective username.
     *
     * @param protocol
     * @param username
     * @param password
     * @param ipAddress
     * @param serverConf
     * @return
     * @throws HplsqlException
     */
    public SessionHandle openSession(TProtocolVersion protocol, String username, String password, String ipAddress,
                                     ServerConf serverConf)
            throws HplsqlException {
        return createSession(null, protocol, username, password, ipAddress, serverConf).getSessionHandle();
    }

    public HplsqlSession createSession(SessionHandle sessionHandle, TProtocolVersion protocol, String username,
                                       String password, String ipAddress, ServerConf serverConf)
            throws HplsqlException{
        incrementConnections(username, ipAddress);
        HplsqlSession session = new HplsqlSessionImpl(sessionHandle, protocol, username, password, ipAddress);
        session.setSessionManager(this);
        session.setOperationManager(operationManager);
        try {
            session.open(serverConf);
        } catch (Exception e) {
            LOG.warn("Failed to open session", e);
            try {
                session.close();
            } catch (Throwable t) {
                LOG.warn("Error closing session", t);
            }
            throw new HplsqlException("Failed to open new session: " + e.getMessage(), e);
        }
        handleToSession.put(session.getSessionHandle(), session);
        LOG.info("Session opened, " + session.getSessionHandle() + ", current sessions:" + getOpenSessionCount());
        return session;
    }

    public void closeSession(SessionHandle sessionHandle) throws HplsqlException{
        HplsqlSession session = handleToSession.remove(sessionHandle);
        if(session == null){
            throw new HplsqlException("Session does not exist: " + sessionHandle);
        }
        session.close();
        decrementConnections(session);
    }

    public int getOpenSessionCount() {
        return handleToSession.size();
    }

    /**
     * 验证是否达到连接限制，达到上限则抛出异常。未达到上限则增加连接数
     *
     * @param username
     * @param ipAddress
     * @throws HplsqlException
     */
    private void incrementConnections(final String username, final String ipAddress) throws HplsqlException {
        String violation = anyViolations(username, ipAddress);
        // increment the counters only when there are no violations
        if (violation == null) {
            if (trackConnectionsPerUser(username)) {
                connectionsCount.computeIfAbsent(username, k -> new LongAdder()).increment();
            }

            if (trackConnectionsPerIpAddress(ipAddress)) {
                connectionsCount.computeIfAbsent(ipAddress, k -> new LongAdder()).increment();
            }

            if (trackConnectionsPerUserIpAddress(username, ipAddress)) {
                connectionsCount.computeIfAbsent(username + ":" + ipAddress, k -> new LongAdder()).increment();
            }
        } else {
            LOG.error(violation);
            throw new HplsqlException(violation);
        }
    }

    private void decrementConnections(final HplsqlSession session) {
        final String username = session.getUserName();
        final String clientIpAddress = session.getIpAddress();
        if (trackConnectionsPerUser(username)) {
            connectionsCount.computeIfPresent(username, (k, v) -> v).decrement();
        }

        if (trackConnectionsPerIpAddress(clientIpAddress)) {
            connectionsCount.computeIfPresent(clientIpAddress, (k, v) -> v).decrement();
        }

        if (trackConnectionsPerUserIpAddress(username, clientIpAddress)) {
            connectionsCount.computeIfPresent(username + ":" + clientIpAddress, (k, v) -> v).decrement();
        }
    }

    private String anyViolations(final String username, final String ipAddress) {
        if (trackConnectionsPerUser(username) && !withinLimits(username, userLimit)) {
            return "Connection limit per user reached (user: " + username + " limit: " + userLimit + ")";
        }

        if (trackConnectionsPerIpAddress(ipAddress) && !withinLimits(ipAddress, ipAddressLimit)) {
            return "Connection limit per ipaddress reached (ipaddress: " + ipAddress + " limit: " + ipAddressLimit + ")";
        }

        if (trackConnectionsPerUserIpAddress(username, ipAddress) &&
                !withinLimits(username + ":" + ipAddress, userIpAddressLimit)) {
            return "Connection limit per user:ipaddress reached (user:ipaddress: " + username + ":" + ipAddress + " limit: " +
                    userIpAddressLimit + ")";
        }
        return null;
    }

    private boolean trackConnectionsPerUserIpAddress(final String username, final String ipAddress) {
        return userIpAddressLimit > 0 && username != null && !username.isEmpty() && ipAddress != null &&
                !ipAddress.isEmpty();
    }

    private boolean trackConnectionsPerIpAddress(final String ipAddress) {
        return ipAddressLimit > 0 && ipAddress != null && !ipAddress.isEmpty();
    }

    private boolean trackConnectionsPerUser(final String username) {
        return userLimit > 0 && username != null && !username.isEmpty();
    }

    private boolean withinLimits(final String track, final int limit) {
        if (connectionsCount.containsKey(track)) {
            final int connectionCount = connectionsCount.get(track).intValue();
            if (connectionCount >= limit) {
                return false;
            }
        }
        return true;
    }

    public HplsqlSession getSession(SessionHandle sessionHandle) throws HplsqlException {
        HplsqlSession session = handleToSession.get(sessionHandle);
        if (session == null) {
            throw new HplsqlException("Invalid SessionHandle: " + sessionHandle);
        }
        return session;
    }

    public OperationManager getOperationManager() {
        return operationManager;
    }


}
