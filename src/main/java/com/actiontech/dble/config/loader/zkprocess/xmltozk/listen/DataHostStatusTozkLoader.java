package com.actiontech.dble.config.loader.zkprocess.xmltozk.listen;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;

import java.util.Map;

/**
 * Created by szf on 2019/10/30.
 */
public class DataHostStatusTozkLoader extends ZkMultiLoader implements NotifyService {


    public DataHostStatusTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        HaConfigManager.getInstance().init();
        if ("true".equals(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_CLUSTER_HA))) {
            Map<String, String> map = HaConfigManager.getInstance().getSourceJsonList();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                this.checkAndWriteString(KVPathUtil.getHaStatusPath(), entry.getKey(), entry.getValue());
            }
        }
        return true;
    }
}
