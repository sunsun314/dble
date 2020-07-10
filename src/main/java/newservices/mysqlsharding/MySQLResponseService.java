package newservices.mysqlsharding;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import newbootstrap.DbleServer;
import newcommon.proto.mysql.packet.PingPacket;
import newcommon.service.ServiceTask;
import newnet.connection.AbstractConnection;
import newnet.connection.BackendConnection;
import newservices.MySQLBasedService;
import newservices.mysqlsharding.backend.nio.handler.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2020/6/29.
 */
public class MySQLResponseService extends MySQLBasedService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLResponseService.class);
    private static final int RESULT_STATUS_INIT = 0;
    private static final int RESULT_STATUS_HEADER = 1;
    private static final int RESULT_STATUS_FIELD_EOF = 2;

    private ResponseHandler handler;

    protected final AtomicBoolean isHandling = new AtomicBoolean(false);

    private volatile int resultStatus;

    private volatile Object attachment;

    public MySQLResponseService(AbstractConnection connection) {
        super(connection);
    }

    @Override
    public void handleData(ServiceTask task) {
        handleInnerData(task.getOrgData());
    }

    @Override
    protected void handleInnerData(byte[] data) {
        //todo finish this
    }

    protected void TaskToTotalQueue(ServiceTask task) {
        handleQueue(DbleServer.getInstance().getBackendBusinessExecutor());
    }

    protected void handleQueue(final Executor executor) {
        if (isHandling.compareAndSet(false, true)) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        handleInnerData();
                    } catch (Exception e) {
                        handleDataError(e);
                    } finally {
                        isHandling.set(false);
                        if (taskQueue.size() > 0) {
                            handleQueue(executor);
                        }
                    }
                }
            });
        }
    }

    protected void handleDataError(Exception e) {
        LOGGER.info(this.toString() + " handle data error:", e);
        while (taskQueue.size() > 0) {
            taskQueue.clear();
            // clear all data from the client
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
        }
        resultStatus = RESULT_STATUS_INIT;
        connection.close("handle data error:" + e.getMessage());
    }


    private void handleInnerData() {
        ServiceTask task;

        //threadUsageStat start
        String threadName = null;
        ThreadWorkUsage workUsage = null;
        long workStart = 0;
        if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
            threadName = Thread.currentThread().getName();
            workUsage = DbleServer.getInstance().getThreadUsedMap().get(threadName);
            if (threadName.startsWith("backend")) {
                if (workUsage == null) {
                    workUsage = new ThreadWorkUsage();
                    DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
                }
            }

            workStart = System.nanoTime();
        }
        //handleData
        while ((task = taskQueue.poll()) != null) {
            handleData(task);
        }
        //threadUsageStat end
        if (workUsage != null && threadName.startsWith("backend")) {
            workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
        }
    }

    public BackendConnection getConnection() {
        return (BackendConnection) connection;
    }

    public void setResponseHandler(ResponseHandler handler) {
        this.handler = handler;
    }

    public void ping() {
        this.write(PingPacket.PING);
    }

    public Object getAttachment() {
        return attachment;
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

}
