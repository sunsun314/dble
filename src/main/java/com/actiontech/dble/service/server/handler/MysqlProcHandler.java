/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.server.handler;


import com.actiontech.dble.common.mysql.util.PacketUtil;
import com.actiontech.dble.common.config.Fields;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.service.server.ServerConnection;

public final class MysqlProcHandler {
    private MysqlProcHandler() {
    }

    private static final int FIELD_COUNT = 2;
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    static {
        FIELDS[0] = PacketUtil.getField("name",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[1] = PacketUtil.getField("type", Fields.FIELD_TYPE_VAR_STRING);
    }

    public static void handle(ServerConnection c) {
        MysqlSystemSchemaHandler.doWrite(FIELD_COUNT, FIELDS, null, c);
    }


}
