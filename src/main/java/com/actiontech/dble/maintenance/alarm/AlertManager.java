package com.actiontech.dble.server.status;

import com.actiontech.dble.maintenance.alarm.AlertBlockQueue;
import com.actiontech.dble.maintenance.alarm.AlertSender;
import com.actiontech.dble.maintenance.alarm.AlertTask;
import com.actiontech.dble.common.util.ExecutorUtil;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Created by szf on 2019/3/25.
 */
public class AlertManager {
    private BlockingQueue<AlertTask> alertQueue = new AlertBlockQueue<>(1024);
    private static final AlertManager INSTANCE = new AlertManager();
    private ExecutorService alertSenderExecutor;

    public static AlertManager getInstance() {
        return INSTANCE;
    }

    public BlockingQueue<AlertTask> getAlertQueue() {
        return alertQueue;
    }

    public void startAlert() {
        alertSenderExecutor = ExecutorUtil.createCached("alertSenderExecutor", 1);
        alertSenderExecutor.execute(new AlertSender(alertQueue));
    }


}
