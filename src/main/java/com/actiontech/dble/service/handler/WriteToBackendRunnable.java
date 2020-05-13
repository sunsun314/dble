/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.handler;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.common.mysql.packet.WriteToBackendTask;
import com.actiontech.dble.assistant.statistic.stat.ThreadWorkUsage;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class WriteToBackendRunnable implements Runnable {
    private final BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue;

    public WriteToBackendRunnable(BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue) {
        this.writeToBackendQueue = writeToBackendQueue;
    }

    @Override
    public void run() {
        ThreadWorkUsage workUsage = null;
        if (DbleServer.getInstance().getConfig().getSystem().getUseThreadUsageStat() == 1) {
            String threadName = Thread.currentThread().getName();
            workUsage = new ThreadWorkUsage();
            DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
        }
        while (true) {
            try {
                List<WriteToBackendTask> tasks = writeToBackendQueue.take();
                //threadUsageStat start
                long workStart = 0;
                if (workUsage != null) {
                    workStart = System.nanoTime();
                }

                //execute the tasks
                for (WriteToBackendTask task : tasks) {
                    task.execute();
                }

                //threadUsageStat end
                if (workUsage != null) {
                    workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("FrontendCommandHandler error.", e);
            }
        }

    }
}
