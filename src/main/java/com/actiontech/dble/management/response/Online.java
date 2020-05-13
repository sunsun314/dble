/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.management.response;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.management.ManagerConnection;
import com.actiontech.dble.common.mysql.packet.OkPacket;

/**
 * @author mycat
 */
public final class Online {
    private Online() {
    }

    private static final OkPacket OK = new OkPacket();

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1);
        OK.setServerStatus(2);
    }

    public static void execute(ManagerConnection mc) {
        DbleServer.getInstance().online();
        OK.write(mc);
    }

}
