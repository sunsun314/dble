/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.management.response;

import com.actiontech.dble.common.mysql.util.PacketUtil;
import com.actiontech.dble.common.config.Fields;
import com.actiontech.dble.service.manager.ManagerConnection;
import com.actiontech.dble.common.mysql.packet.EOFPacket;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.ResultSetHeaderPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.assistant.statistic.stat.UserSqlLastStat;
import com.actiontech.dble.assistant.statistic.stat.UserStat;
import com.actiontech.dble.assistant.statistic.stat.UserStatAnalyzer;
import com.actiontech.dble.common.util.FormatUtil;
import com.actiontech.dble.common.util.LongUtil;
import com.actiontech.dble.common.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;


/**
 * Show Last SQL
 *
 * @author mycat
 * @author zhuam
 */
public final class ShowSQL {
    private ShowSQL() {
    }

    private static final int FIELD_COUNT = 5;
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

        FIELDS[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c, boolean isClear) {
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
        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            String user = userStat.getUser();
            List<UserSqlLastStat.SqlLast> queries = userStat.getSqlLastStat().getQueries();
            int i = 1;
            for (UserSqlLastStat.SqlLast sqlLast : queries) {
                if (sqlLast != null) {
                    RowDataPacket row = getRow(user, sqlLast, i, c.getCharset().getResults());
                    row.setPacketId(++packetId);
                    i++;
                    buffer = row.write(buffer, c, true);
                }
            }

            if (isClear) {
                userStat.getSqlLastStat().clear();
            }
        }


        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String user, UserSqlLastStat.SqlLast sql, int idx, String charset) {

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));
        row.add(StringUtil.encode(user, charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(sql.getStartTime()), charset));
        row.add(LongUtil.toBytes(sql.getExecuteTime()));
        row.add(StringUtil.encode(sql.getSql(), charset));
        return row;
    }

}
