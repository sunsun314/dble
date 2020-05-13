/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.response;

import com.actiontech.dble.common.mysql.packet.OkPacket;
import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.service.ServerConnection;
import com.actiontech.dble.sql.parser.ScriptPrepareParse;

import java.util.List;
import java.util.LinkedList;

public final class SptPrepare {
    private SptPrepare() {
    }

    private static boolean checksync(String stmt, ServerConnection c) {
        return true;
    }

    public static void response(ServerConnection c) {
        String stmt = c.getSptPrepare().getExePrepare();
        if (stmt == null) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "SQL syntax error");
            return;
        }

        if (checksync(stmt, c)) {
            String name = c.getSptPrepare().getName();
            List<String> args = new LinkedList();
            ScriptPrepareParse.parseStmt(stmt, args);
            c.getSptPrepare().setPrepare(name, args);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "SQL syntax error");
        }
    }
}
