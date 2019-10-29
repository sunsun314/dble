package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
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

import static com.actiontech.dble.util.KVPathUtil.SEPARATOR;

/**
 * Created by szf on 2019/10/22.
 */
public class DataHostDisable {

    public static void execute(Matcher disable, ManagerConnection mc) {
        String dhName = disable.group(1);
        String subHostName = disable.group(3);
        boolean useCluster = "true".equals(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));

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
                disableWithCluster(dh, subHostName, mc);
            } else {
                //dble start in single mode
                dh.disableHosts(subHostName, true);
            }

            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(mc);
        } else {
            mc.writeErrMessage(ErrorCode.ER_YES, "dataHost mod not allowed to disable");
        }
    }


    public static void disableWithCluster(PhysicalDNPoolSingleWH dh, String subHostName, ManagerConnection mc) {
        //get the lock from ucore
        DistributeLock distributeLock = new DistributeLock(ClusterPathUtil.getHaLockPath(dh.getHostName()),
                new HaInfo(dh.getHostName(),
                        ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                        HaInfo.HaType.DATAHOST_DISABLE,
                        HaInfo.HaStatus.INIT
                ).toString()
        );
        try {
            if (!distributeLock.acquire()) {
                mc.writeErrMessage(ErrorCode.ER_YES, "Other instance is changing the dataHost, please try again later.");
                return;
            }
            dh.disableHosts(subHostName, false);
            ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(dh.getHostName()), dh.getClusterHaJson());

            ClusterHelper.setKV(ClusterPathUtil.getHaLockPath(dh.getHostName()),
                    new HaInfo(dh.getHostName(),
                            ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                            HaInfo.HaType.DATAHOST_DISABLE,
                            HaInfo.HaStatus.SUCCESS
                    ).toString());
            String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, ClusterPathUtil.getHaLockPath(dh.getHostName()) + SEPARATOR);
            if (errorMsg != null) {
                throw new RuntimeException(errorMsg);
            }
        } catch (Exception e) {
            mc.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
            return;
        } finally {
            ClusterHelper.cleanPath(ClusterPathUtil.getHaLockPath(dh.getHostName()) + SEPARATOR);
            distributeLock.release();
        }
    }
}
