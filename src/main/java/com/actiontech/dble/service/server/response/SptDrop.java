/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.response;

import com.actiontech.dble.common.mysql.packet.OkPacket;
import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.service.ServerConnection;

public final class SptDrop {
    private SptDrop() {
    }

    public static void response(ServerConnection c) {
        String name = c.getSptPrepare().getName();
        if (c.getSptPrepare().delPrepare(name)) {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_STMT_HANDLER, "Unknown prepared statement handler" + name + " given to DEALLOCATE PREPARE");
        }
    }
}
