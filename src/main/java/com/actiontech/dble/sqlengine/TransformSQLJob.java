/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.util.StringUtil;
import newcommon.proto.mysql.packet.*;
import newservices.manager.ManagerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TransformSQLJob implements ResponseHandler, Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransformSQLJob.class);
    private final String sql;
    private final String databaseName;
    private final PhysicalDbInstance ds;
    private final ManagerService service;
    private BackendConnection connection;

    public TransformSQLJob(String sql, String databaseName, PhysicalDbInstance ds, ManagerService mc) {
        this.sql = sql;
        this.databaseName = databaseName;
        this.ds = ds;
        this.service = mc;
    }

    @Override
    public void run() {
        try {
            if (ds == null) {
                RouteResultsetNode node = new RouteResultsetNode(databaseName, ServerParse.SELECT, sql);
                // create new connection
                ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), false, true, node, this, node);
            } else {
                ds.getConnection(databaseName, true, this, null, false);
            }
        } catch (Exception e) {
            LOGGER.warn("can't get connection", e);
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setPacketId(1);
            errPacket.setErrNo(ErrorCode.ER_YES);
            errPacket.setMessage(StringUtil.encode(e.toString(), StandardCharsets.UTF_8.toString()));
            writeError(errPacket.toBytes());
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("can't get connection for sql :" + sql, e);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_YES);
        errPacket.setMessage(StringUtil.encode(e.toString(), StandardCharsets.UTF_8.toString()));
        writeError(errPacket.toBytes());
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.setResponseHandler(this);
        connection = conn;
        try {
            ((MySQLConnection) conn).sendQueryCmd(sql, service.getCharset());
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setPacketId(1);
            errPacket.setErrNo(ErrorCode.ER_YES);
            errPacket.setMessage(StringUtil.encode(e.toString(), StandardCharsets.UTF_8.toString()));
            writeError(errPacket.toBytes());
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        writeError(err);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        service.write(ok);
        connection.release();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        service.write(header);
        for (byte[] field : fields) {
            service.write(field);
        }
        service.write(eof);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        service.write(row);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        service.write(eof);
        connection.release();
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_YES);
        errPacket.setMessage(StringUtil.encode(reason, StandardCharsets.UTF_8.toString()));
        writeError(errPacket.toBytes());
    }

    private void writeError(byte[] err) {
        service.write(err);
        if (connection != null) {
            connection.release();
        }
    }
}
