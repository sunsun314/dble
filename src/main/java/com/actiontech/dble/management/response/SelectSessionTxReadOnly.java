/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.assistant.backend.mysql.PacketUtil;
import com.actiontech.dble.common.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.common.mysql.packet.EOFPacket;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.ResultSetHeaderPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.common.util.LongUtil;

import java.nio.ByteBuffer;

public final class SelectSessionTxReadOnly {
    private SelectSessionTxReadOnly() {
    }


    public static void execute(ManagerConnection c, String column) {
        ByteBuffer buffer = c.allocate();
        byte packetId = 0;
        ResultSetHeaderPacket header = PacketUtil.getHeader(1);
        header.setPacketId(++packetId);
        // write header
        buffer = header.write(buffer, c, true);
        FieldPacket[] fields = new FieldPacket[1];
        fields[0] = PacketUtil.getField(column, Fields.FIELD_TYPE_INT24);
        fields[0].setPacketId(++packetId);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        // write eof
        buffer = eof.write(buffer, c, true);

        // write rows
        RowDataPacket row = new RowDataPacket(1);
        row.setPacketId(++packetId);
        row.add(LongUtil.toBytes(0));
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

}
