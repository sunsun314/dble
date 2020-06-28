package newcommon.service;

import newbootstrap.DbleServer;
import newcommon.proto.handler.ProtoHandler;
import newcommon.proto.handler.ProtoHandlerResult;
import newcommon.proto.mysql.packet.ErrorPacket;
import newnet.connection.AbstractConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by szf on 2020/6/16.
 */
public abstract class AbstractService implements Service {

    protected final ConcurrentLinkedQueue<ServiceTask> taskQueue = new ConcurrentLinkedQueue<>();
    protected volatile ProtoHandler proto;

    private final AtomicBoolean executing = new AtomicBoolean(false);


    protected final AbstractConnection connection;

    private volatile int packetId = 0;

    public AbstractService(AbstractConnection connection) {
        this.connection = connection;
    }

    @Override
    public void handle(ByteBuffer dataBuffer) {

        boolean hasReming = true;
        int offset = 0;
        while (hasReming) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset);
            switch (result.getCode()) {
                case REACH_END_BUFFER:
                    connection.readReachEnd();
                    byte[] packetData = result.getPacketData();
                    if (packetData != null) {
                        TaskCreate(packetData);
                    }
                    break;
                case BUFFER_PACKET_UNCOMPLETE:
                    connection.compactReadBuffer(dataBuffer, result.getOffset());
                    hasReming = false;
                    break;
                case BUFFER_NOT_BIG_ENOUGH:
                    connection.ensureFreeSpaceOfReadBuffer(dataBuffer, result.getOffset(), result.getPacketLength());
                    hasReming = false;
                    break;
                case STLL_DATA_REMING:
                    TaskCreate(result.getPacketData());
                    continue;
            }
        }
    }


    private void TaskCreate(byte[] packetData) {
        ServiceTask task = new ServiceTask(packetData, this);
        taskQueue.offer(task);
        //向总队列提供这个task
        TaskToTotalQueue(task);
    }

    private void TaskToTotalQueue(ServiceTask task) {
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    public void execute(ServiceTask task) {
        task.increasePriority();
        if (executing.compareAndSet(false, true)) {
            ServiceTask realTask = taskQueue.poll();
            if (realTask == task) {
                handleData(realTask);
            } else {
                TaskToTotalQueue(task);
            }
        } else {
            TaskToTotalQueue(task);
        }
    }

    public void register() throws IOException {

    }

    public abstract void handleData(ServiceTask task);

    public int nextPacketId() {
        return ++packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public AbstractConnection getConnection() {
        return connection;
    }

    public ByteBuffer allocate() {
        return this.connection.allocate();
    }

    public void write(ByteBuffer buffer) {
        this.connection.write(buffer);
    }



}
