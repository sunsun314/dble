/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.management.response;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.common.mysql.util.PacketUtil;
import com.actiontech.dble.common.config.Fields;
import com.actiontech.dble.service.manager.ManagerConnection;
import com.actiontech.dble.common.mysql.packet.EOFPacket;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.ResultSetHeaderPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.assistant.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.common.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public final class ShowThreadUsed {
    private ShowThreadUsed() {
    }
    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("THREAD_NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LAST_QUARTER_MIN", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LAST_MINUTE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LAST_FIVE_MINUTE", Fields.FIELD_TYPE_VAR_STRING);
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
        Map<String, ThreadWorkUsage> threadUsedMap = new TreeMap<>(DbleServer.getInstance().getThreadUsedMap());
        for (Map.Entry<String, ThreadWorkUsage> entry : threadUsedMap.entrySet()) {
            RowDataPacket row = getRow(entry.getKey(), entry.getValue(), c.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);
        c.write(buffer);
    }

    private static RowDataPacket getRow(String name, ThreadWorkUsage workUsage, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(name, charset));
        String[] workUsages = workUsage.getUsedPercent();
        row.add(StringUtil.encode(workUsages[0], charset));
        row.add(StringUtil.encode(workUsages[1], charset));
        row.add(StringUtil.encode(workUsages[2], charset));
        return row;
    }
}