/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.management.response;

import com.actiontech.dble.assistant.backend.mysql.PacketUtil;
import com.actiontech.dble.common.config.Fields;
import com.actiontech.dble.management.ManagerConnection;
import com.actiontech.dble.common.mysql.packet.EOFPacket;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.ResultSetHeaderPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.singleton.PauseDatanodeManager;
import com.actiontech.dble.common.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2018/8/16.
 */
public final class ShowPauseInfo {

    private ShowPauseInfo() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PAUSE_DATANODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }


    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = HEADER.write(buffer, c, true);
        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        buffer = EOF.write(buffer, c, true);
        // write rows
        byte packetId = EOF.getPacketId();
        if (PauseDatanodeManager.getInstance().getDataNodes() != null) {
            for (String dataNode : PauseDatanodeManager.getInstance().getDataNodes()) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.setPacketId(++packetId);
                row.add(StringUtil.encode(dataNode, c.getCharset().getResults()));
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }


}
