/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.management.response;

import com.actiontech.dble.assistant.backend.BackendConnection;
import com.actiontech.dble.common.mysql.util.PacketUtil;
import com.actiontech.dble.assistant.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.common.config.Fields;
import com.actiontech.dble.service.manager.ManagerConnection;
import com.actiontech.dble.common.net.NIOProcessor;
import com.actiontech.dble.common.mysql.packet.EOFPacket;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.ResultSetHeaderPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.common.util.IntegerUtil;
import com.actiontech.dble.common.util.LongUtil;
import com.actiontech.dble.common.util.StringUtil;
import com.actiontech.dble.common.util.TimeUtil;

import java.nio.ByteBuffer;

/**
 * Show Backend Old connection for reload @@config_all
 *
 * @author zhuam
 */
public final class ShowBackendOld {
    private ShowBackendOld() {
    }

    private static final int FIELD_COUNT = 10;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("BACKEND_ID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("MYSQLID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LOACL_TCP_PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("ACTIVE_TIME(S)", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LASTTIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("BORROWED", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();
        buffer = HEADER.write(buffer, c, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        buffer = EOF.write(buffer, c, true);
        byte packetId = EOF.getPacketId();

        for (BackendConnection bc : NIOProcessor.BACKENDS_OLD) {
            if (bc != null) {
                RowDataPacket row = getRow(bc, c.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);
        c.write(buffer);
    }

    private static RowDataPacket getRow(BackendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(c.getId()));
        long threadId = 0;
        if (c instanceof MySQLConnection) {
            threadId = ((MySQLConnection) c).getThreadId();
        }
        row.add(LongUtil.toBytes(threadId));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(IntegerUtil.toBytes(c.getPort()));
        row.add(IntegerUtil.toBytes(c.getLocalPort()));
        row.add(LongUtil.toBytes(c.getNetInBytes()));
        row.add(LongUtil.toBytes(c.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
        row.add(LongUtil.toBytes(c.getLastTime()));
        boolean isBorrowed = c.isBorrowed();
        row.add(isBorrowed ? "true".getBytes() : "false".getBytes());
        return row;
    }

}
