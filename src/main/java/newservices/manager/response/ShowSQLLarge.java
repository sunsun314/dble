/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.manager.response;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.statistic.stat.UserSqlLargeStat;
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
 * ShowSQLLarge
 *
 * @author zhuam
 */
public final class ShowSQLLarge {
    private ShowSQLLarge() {
    }

    private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ROWS", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
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

            List<UserSqlLargeStat.SqlLarge> queries = userStat.getSqlLargeRowStat().getQueries();
            for (UserSqlLargeStat.SqlLarge sql : queries) {
                if (sql != null) {
                    RowDataPacket row = getRow(user, sql, service.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }

            if (isClear) {
                userStat.getSqlLargeRowStat().clear();
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, service, true);

        // write buffer
        service.write(buffer);
    }

    private static RowDataPacket getRow(Pair<String, String> user, UserSqlLargeStat.SqlLarge sql, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(user.toString(), charset));
        row.add(LongUtil.toBytes(sql.getSqlRows()));
        row.add(StringUtil.encode(FormatUtil.formatDate(sql.getStartTime()), charset));
        row.add(LongUtil.toBytes(sql.getExecuteTime()));
        row.add(StringUtil.encode(sql.getSql(), charset));
        return row;
    }
}

