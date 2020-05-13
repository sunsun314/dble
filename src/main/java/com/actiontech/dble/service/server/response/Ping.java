/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.response;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.assistant.backend.mysql.PacketUtil;
import com.actiontech.dble.common.net.FrontendConnection;
import com.actiontech.dble.common.mysql.packet.ErrorPacket;
import com.actiontech.dble.common.mysql.packet.OkPacket;

/**
 * for heartbeat.
 *
 * @author mycat
 */
public final class Ping {
    private Ping() {
    }

    private static final ErrorPacket ERROR = PacketUtil.getShutdown();

    public static void response(FrontendConnection c) {
        if (DbleServer.getInstance().isOnline()) {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            ERROR.write(c);
        }
    }

}
