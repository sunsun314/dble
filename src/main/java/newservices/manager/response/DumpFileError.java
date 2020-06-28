/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.manager.response;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.util.StringUtil;
import newcommon.proto.mysql.packet.*;
import newcommon.proto.mysql.util.PacketUtil;
import newservices.manager.ManagerService;
import newservices.manager.dump.ErrorMsg;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by baofengqi on 2018/8/6.
 */
public final class DumpFileError {

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TABLE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DETAIL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }


    private DumpFileError() {
    }

    public static void execute(ManagerService service, List<ErrorMsg> errors) {
        ByteBuffer buffer = service.allocate();
        buffer = HEADER.write(buffer, service, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }
        buffer = EOF.write(buffer, service, true);
        byte packetId = EOF.getPacketId();
        for (ErrorMsg err : errors) {
            RowDataPacket row = getRow(err, service.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, service, true);
        service.write(buffer);

    }

    private static RowDataPacket getRow(ErrorMsg err, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(err.getTarget(), charset));
        row.add(StringUtil.encode(err.getErrorMeg(), charset));
        return row;
    }

}
