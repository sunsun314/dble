/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package newservices.mysqlsharding.backend.nio.handler;

import com.actiontech.dble.backend.pool.util.TimerHolder;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import newnet.connection.PooledConnection;
import newnet.pool.PooledConnectionListener;
import newservices.mysqlsharding.MySQLResponseService;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * heartbeat check for mysql connections
 *
 * @author wuzhih
 */
public class ConnectionHeartBeatHandler implements ResponseHandler {

    private final Object heartbeatLock;
    private volatile Timeout heartbeatTimeout;
    private final PooledConnection conn;
    private final PooledConnectionListener listener;
    private boolean finished = false;

    public ConnectionHeartBeatHandler(PooledConnection conn, boolean isBlock, PooledConnectionListener listener) {
        ((MySQLResponseService) conn.getService()).setResponseHandler(this);
        this.conn = conn;
        this.listener = listener;
        if (isBlock) {
            this.heartbeatLock = new Object();
        } else {
            this.heartbeatLock = null;
        }
    }

    public boolean ping(long timeout) {
        ((MySQLResponseService)conn.getService()).ping();
        if (heartbeatLock != null) {
            synchronized (heartbeatLock) {
                try {
                    heartbeatLock.wait(timeout);
                } catch (InterruptedException e) {
                    finished = false;
                }
            }
            return finished;
        } else {
            heartbeatTimeout = TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    conn.businessClose("conn heart timeout");
                }
            }, timeout, TimeUnit.MILLISECONDS);
            return true;
        }
    }

    /**
     * if the query returns ok than just release the connection
     * and go on check the next one
     *
     * @param ok
     */
    @Override
    public void okResponse(byte[] ok, MySQLResponseService service) {
        if (heartbeatLock != null) {
            synchronized (heartbeatLock) {
                finished = true;
                heartbeatLock.notifyAll();
            }
            return;
        }

        heartbeatTimeout.cancel();
        listener.onHeartbeatSuccess(service.getConnection());
    }

    /**
     * if heart beat returns error than clase the connection and
     * start the next one
     *
     * @param data
     * @param con
     */
    @Override
    public void errorResponse(byte[] data, MySQLResponseService con) {
    }

    /**
     * if when the query going on the conneciton be closed
     * than just do nothing and go on for next one
     *
     * @param service
     * @param reason
     */
    @Override
    public void connectionClose(MySQLResponseService service, String reason) {

    }

    /**
     * @param eof
     * @param isLeft
     */
    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, MySQLResponseService service) {
        // not called
    }


    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, MySQLResponseService service) {
        // not called
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, MySQLResponseService service) {
        // not called
        return false;
    }

    @Override
    public void connectionAcquired(MySQLResponseService service) {
        // not called
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        // not called
    }
}
