package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;

import java.util.regex.Matcher;

/**
 * Created by szf on 2019/10/22.
 */
public class DataHostEnable {

    public static void execute(Matcher enable, ManagerConnection mc) {
        String dhName = enable.group(1);
        String subHostName = enable.group(3);

        //check the dataHost is exists

        AbstractPhysicalDBPool dataHost = DbleServer.getInstance().getConfig().getDataHosts().get(dhName);
        if (dataHost == null) {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
            return;
        }

        if (dataHost instanceof PhysicalDNPoolSingleWH) {
            PhysicalDNPoolSingleWH dh = (PhysicalDNPoolSingleWH) dataHost;
            dh.enableHosts(subHostName, true);

            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(mc);
        } else {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
        }
    }
}
