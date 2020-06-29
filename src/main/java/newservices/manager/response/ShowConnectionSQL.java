/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package newservices.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import newcommon.proto.mysql.packet.*;
import newcommon.proto.mysql.util.PacketUtil;
import newservices.manager.ManagerService;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class ShowConnectionSQL {
    private ShowConnectionSQL() {
    }

    private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FRONT_ID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("STAGE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service) {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);

        // write rows
        byte packetId = EOF.getPacketId();
        for (NIOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (!fc.isClosed()) {
                    RowDataPacket row = getRow(fc, service.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, service, true);

        // write buffer
        service.write(buffer);
    }

    private static RowDataPacket getRow(FrontendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(c.getId()));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(StringUtil.encode(c.getUserInfo(), charset));
        if (c instanceof ServerConnection) {
            row.add(StringUtil.encode(((ServerConnection) c).getSchema(), charset));
        } else {
            row.add(StringUtil.encode("", charset));
        }
        row.add(StringUtil.encode(FormatUtil.formatDate(c.getLastReadTime()), charset));
        long rt = c.getLastReadTime();
        long wt = c.getLastWriteTime();
        row.add(LongUtil.toBytes((wt >= rt) ? (wt - rt) : (TimeUtil.currentTimeMillis() - rt)));
        if (c instanceof ServerConnection) {
            String executeSql = "";
            ServerConnection sc = (ServerConnection) c;
            if (sc.getExecuteSql() != null) {
                executeSql = sc.getExecuteSql().length() <= 1024 ? sc.getExecuteSql() : sc.getExecuteSql().substring(0, 1024);
            }
            row.add(StringUtil.encode(executeSql, charset));
            row.add(StringUtil.encode(sc.getSession2().getSessionStage().toString(), charset));
        } else {
            row.add(StringUtil.encode("", charset));
            row.add(StringUtil.encode("Manager connection", charset));
        }
        return row;
    }

}
