/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package newservices.manager.response;

import com.actiontech.dble.DbleServer;
import newcommon.proto.mysql.packet.OkPacket;
import newservices.manager.ManagerService;

/**
 * @author mycat
 */
public final class Offline {
    private Offline() {
    }

    private static final OkPacket OK = new OkPacket();

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1);
        OK.setServerStatus(2);
    }

    public static void execute(ManagerService service) {
        DbleServer.getInstance().offline();
        OK.write(service);
    }

}
