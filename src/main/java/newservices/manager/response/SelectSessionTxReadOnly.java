/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.manager.response;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.util.LongUtil;
import newcommon.proto.mysql.packet.*;
import newcommon.proto.mysql.util.PacketUtil;
import newservices.manager.ManagerService;

import java.nio.ByteBuffer;

public final class SelectSessionTxReadOnly {
    private SelectSessionTxReadOnly() {
    }


    public static void execute(ManagerService service, String column) {
        ByteBuffer buffer = service.allocate();
        byte packetId = 0;
        ResultSetHeaderPacket header = PacketUtil.getHeader(1);
        header.setPacketId(++packetId);
        // write header
        buffer = header.write(buffer, service, true);
        FieldPacket[] fields = new FieldPacket[1];
        fields[0] = PacketUtil.getField(column, Fields.FIELD_TYPE_INT24);
        fields[0].setPacketId(++packetId);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, service, true);
        }
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        // write eof
        buffer = eof.write(buffer, service, true);

        // write rows
        RowDataPacket row = new RowDataPacket(1);
        row.setPacketId(++packetId);
        row.add(LongUtil.toBytes(service.getUserConfig().isReadOnly() ? 1 : 0));
        buffer = row.write(buffer, service, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, service, true);

        // post write
        service.write(buffer);
    }

}
