package newservices.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.WriteQueueFlowController;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import newcommon.proto.mysql.packet.*;
import newcommon.proto.mysql.util.PacketUtil;
import newservices.manager.ManagerService;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/4/10.
 */
public final class FlowControlList {

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_ID", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_INFO", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("WRITE_QUEUE_SIZE", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private FlowControlList() {

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

        if (WriteQueueFlowController.isEnableFlowControl()) {
            //find all server connection
            packetId = findAllServerConnection(buffer, service, packetId);
            //find all mysql connection
            packetId = findAllMySQLConeection(buffer, service, packetId);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, service, true);

        // write buffer
        service.write(buffer);
    }


    private static byte findAllServerConnection(ByteBuffer buffer, ManagerService service, byte packetId) {
        NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
        for (NIOProcessor p : processors) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc instanceof ServerConnection && fc.isFlowControlled()) {
                    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                    row.add(StringUtil.encode("ServerConnection", service.getCharset().getResults()));
                    row.add(LongUtil.toBytes(fc.getId()));
                    row.add(StringUtil.encode(fc.getHost() + ":" + fc.getLocalPort() + "/" + ((ServerConnection) fc).getSchema() + " user = " + fc.getUser(), service.getCharset().getResults()));
                    row.add(LongUtil.toBytes(fc.getWriteQueue().size()));
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }
        }
        return packetId;
    }

    private static byte findAllMySQLConeection(ByteBuffer buffer, ManagerService service, byte packetId) {
        NIOProcessor[] processors = DbleServer.getInstance().getBackendProcessors();
        for (NIOProcessor p : processors) {
            for (BackendConnection bc : p.getBackends().values()) {
                MySQLConnection mc = (MySQLConnection) bc;
                if (mc.isFlowControlled()) {
                    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                    row.add(StringUtil.encode("MySQLConnection", service.getCharset().getResults()));
                    row.add(LongUtil.toBytes(mc.getThreadId()));
                    row.add(StringUtil.encode(mc.getPool().getConfig().getUrl() + "/" + mc.getSchema() + " id = " + mc.getThreadId(), service.getCharset().getResults()));
                    row.add(LongUtil.toBytes(mc.getWriteQueue().size()));
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }
        }
        return packetId;
    }
}
