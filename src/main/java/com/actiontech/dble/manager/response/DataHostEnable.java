package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.ClusterGeneralConfig;

import java.util.regex.Matcher;


/**
 * Created by szf on 2019/10/22.
 */
public final class DataHostEnable {

    private DataHostEnable() {

    }

    public static void execute(Matcher enable, ManagerConnection mc) {
        String dhName = enable.group(1);
        String subHostName = enable.group(3);
        boolean useCluster = "true".equals(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_CLUSTER_HA));
        //check the dataHost is exists

        AbstractPhysicalDBPool dataHost = DbleServer.getInstance().getConfig().getDataHosts().get(dhName);
        if (dataHost == null) {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
            return;
        }

        if (dataHost instanceof PhysicalDNPoolSingleWH) {
            PhysicalDNPoolSingleWH dh = (PhysicalDNPoolSingleWH) dataHost;
            if (!dh.checkDataSourceExist(subHostName)) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Some of the dataSource in command in " + dh.getHostName() + " do not exists");
                return;
            }
            if (ClusterGeneralConfig.isUseGeneralCluster() && useCluster) {
                enableWithCluster(dh, subHostName, mc);
            } else {
                //dble start in single mode
                dh.enableHosts(subHostName, true);
            }


            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(mc);
        } else {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
        }
    }


    public static void enableWithCluster(PhysicalDNPoolSingleWH dh, String subHostName, ManagerConnection mc) {
        //get the lock from ucore
        DistributeLock distributeLock = new DistributeLock(ClusterPathUtil.getHaLockPath(dh.getHostName()),
                new HaInfo(dh.getHostName(),
                        ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                        HaInfo.HaType.DATAHOST_ENABLE,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        try {
            if (!distributeLock.acquire()) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dataHost, please try again later.");
                return;
            }
            dh.enableHosts(subHostName, false);
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getHostName()), dh.getClusterHaJson());
        } catch (Exception e) {
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            return;
        } finally {
            distributeLock.release();
        }
    }
}
