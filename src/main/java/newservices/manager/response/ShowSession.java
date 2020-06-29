/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;
import newcommon.proto.mysql.packet.*;
import newcommon.proto.mysql.util.PacketUtil;
import newservices.manager.ManagerService;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * show front session detail info
 *
 * @author wuzhih
 */
public final class ShowSession {
    private ShowSession() {
    }

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FRONT_ID", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DN_COUNT", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DN_LIST", Fields.FIELD_TYPE_VARCHAR);
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
        for (NIOProcessor process : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection front : process.getFrontends().values()) {

                if (!(front instanceof ServerConnection)) {
                    continue;
                }
                ServerConnection sc = (ServerConnection) front;
                RowDataPacket row = getRow(sc, service.getCharset().getResults());
                if (row != null) {
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

    private static RowDataPacket getRow(ServerConnection sc, String charset) {
        StringBuilder sb = new StringBuilder();
        NonBlockingSession session = sc.getSession2();
        Collection<BackendConnection> backConnections = session.getTargetMap().values();
        int cnCount = backConnections.size();
        if (cnCount == 0) {
            return null;
        }
        for (BackendConnection backCon : backConnections) {
            sb.append(backCon).append("\r\n");
        }
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(sc.getId() + "", charset));
        row.add(StringUtil.encode(cnCount + "", charset));
        row.add(StringUtil.encode(sb.toString(), charset));
        return row;
    }
}
