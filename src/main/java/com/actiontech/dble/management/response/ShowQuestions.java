package com.actiontech.dble.management.response;

import com.actiontech.dble.common.mysql.util.PacketUtil;
import com.actiontech.dble.common.config.Fields;
import com.actiontech.dble.service.manager.ManagerConnection;
import com.actiontech.dble.common.mysql.packet.EOFPacket;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.ResultSetHeaderPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.singleton.TsQueriesCounter;
import com.actiontech.dble.common.util.LongUtil;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2019/12/17.
 */
public final class ShowQuestions {

    private ShowQuestions() {
    }

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Questions", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Transactions", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

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


        TsQueriesCounter.CalculateResult result = TsQueriesCounter.getInstance().calculate();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(result.queries));
        row.add(LongUtil.toBytes(result.transactions));
        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);

        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

}
