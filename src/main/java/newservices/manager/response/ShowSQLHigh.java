/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.manager.response;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.statistic.stat.SqlFrequency;
import com.actiontech.dble.statistic.stat.UserStat;
import com.actiontech.dble.statistic.stat.UserStatAnalyzer;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import newcommon.proto.mysql.packet.*;
import newcommon.proto.mysql.util.PacketUtil;
import newservices.manager.ManagerService;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * ShowSQLHigh
 *
 * @author zhuam
 */
public final class ShowSQLHigh {
    private ShowSQLHigh() {
    }

    private static final int FIELD_COUNT = 9;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FREQUENCY", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("AVG_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("MAX_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("MIN_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service, boolean isClear) {
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

        Map<Pair<String, String>, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            Pair<String, String> user = userStat.getUser();
            List<SqlFrequency> list = userStat.getSqlHigh().getSqlFrequency(isClear);
            if (list != null) {
                int i = 1;
                for (SqlFrequency sqlFrequency : list) {
                    if (sqlFrequency != null) {
                        RowDataPacket row = getRow(i, user, sqlFrequency.getSql(), sqlFrequency.getCount(),
                                sqlFrequency.getAvgTime(), sqlFrequency.getMaxTime(), sqlFrequency.getMinTime(),
                                sqlFrequency.getExecuteTime(), sqlFrequency.getLastTime(), service.getCharset().getResults());
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, service, true);
                        i++;
                    }
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

    private static RowDataPacket getRow(int i, Pair<String, String> user, String sql, long count, long avgTime, long maxTime,
                                        long minTime, long executeTime, long lastTime, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(i));
        row.add(StringUtil.encode(user.toString(), charset));
        row.add(LongUtil.toBytes(count));
        row.add(LongUtil.toBytes(avgTime));
        row.add(LongUtil.toBytes(maxTime));
        row.add(LongUtil.toBytes(minTime));
        row.add(LongUtil.toBytes(executeTime));
        row.add(StringUtil.encode(FormatUtil.formatDate(lastTime), charset));
        row.add(StringUtil.encode(sql, charset));
        return row;
    }


}
