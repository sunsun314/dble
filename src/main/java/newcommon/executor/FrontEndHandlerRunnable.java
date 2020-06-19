package newcommon.executor;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import newcommon.service.ServiceTask;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by szf on 2020/6/18.
 */
public class FrontEndHandlerRunnable implements Runnable {

    private final BlockingQueue<ServiceTask> frontNormalTasks;
    private final Queue<ServiceTask> frontPriorityTasks;

    public FrontEndHandlerRunnable(BlockingQueue<ServiceTask> frontEndTasks, BlockingQueue<ServiceTask> frontPriorityTasks) {
        this.frontNormalTasks = frontEndTasks;
        this.frontPriorityTasks = frontPriorityTasks;
    }


    @Override
    public void run() {
        ServiceTask task;
        ThreadWorkUsage workUsage = null;
        if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
            String threadName = Thread.currentThread().getName();
            workUsage = new ThreadWorkUsage();
            DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
        }
        while (true) {
            try {
                task = frontPriorityTasks.poll();
                if (task == null) {
                    task = frontNormalTasks.take();
                }

                //threadUsageStat start
                long workStart = 0;
                if (workUsage != null) {
                    workStart = System.nanoTime();
                }
                //handler data
                task.getService().execute(task);

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
