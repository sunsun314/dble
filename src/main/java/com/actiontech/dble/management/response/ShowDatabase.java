/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.management.response;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.common.mysql.util.PacketUtil;
import com.actiontech.dble.common.config.Fields;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.service.manager.ManagerConnection;
import com.actiontech.dble.common.mysql.packet.EOFPacket;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.ResultSetHeaderPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.common.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeSet;

/**
 * ShowDatabase
 *
 * @author mycat
 * @author mycat
 */
public final class ShowDatabase {
    private ShowDatabase() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DATABASE", Fields.FIELD_TYPE_VAR_STRING);
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
        Map<String, SchemaConfig> schemas = DbleServer.getInstance().getConfig().getSchemas();
        for (String name : new TreeSet<>(schemas.keySet())) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(name, c.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

}
