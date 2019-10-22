package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.singleton.ClusterGeneralConfig;

import java.util.regex.Matcher;

/**
 * Created by szf on 2019/10/22.
 */
public class DataHostDisable {

    public static void execute(Matcher disable, ManagerConnection mc) {
        String dhName = disable.group(1);
        String subHostName = disable.group(3);
        boolean useCluster = true;

        //check the dataHost is exists

        AbstractPhysicalDBPool dataHost = DbleServer.getInstance().getConfig().getDataHosts().get(dhName);
        if (dataHost == null) {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
            return;
        }

        if (dataHost instanceof PhysicalDNPoolSingleWH) {
            PhysicalDNPoolSingleWH dh = (PhysicalDNPoolSingleWH) dataHost;
            if (ClusterGeneralConfig.isUseGeneralCluster() && useCluster) {
                //get the lock from ucore
                try {
                    ClusterHelper.lock("", "");
                } catch (Exception e) {
                    mc.writeErrMessage(ErrorCode.ER_YES, "can not get dataHost lock,other instance is change the config");
                    return;
                }

            } else if (ClusterGeneralConfig.isUseZK() && useCluster) {

            } else {
                //dble start in single mode
                dh.disableHosts(subHostName);
            }
        } else {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
        }
    }

    public static String groupLockJson() {
        return null;
    }
}
