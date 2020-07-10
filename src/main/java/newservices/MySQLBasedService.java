package newservices;

import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.StringUtil;
import newbootstrap.DbleServer;
import newcommon.proto.mysql.packet.CharsetNames;
import newcommon.proto.mysql.packet.ErrorPacket;
import newcommon.service.AbstractService;
import newcommon.service.ServiceTask;
import newnet.connection.AbstractConnection;

/**
 * Created by szf on 2020/6/28.
 */
public abstract class MySQLBasedService extends AbstractService {

    protected UserConfig userConfig;

    protected Pair<String, String> user;

    protected long clientFlags;


    public MySQLBasedService(AbstractConnection connection) {
        super(connection);
    }


    protected void TaskToPriorityQueue(ServiceTask task){
        DbleServer.getInstance().getFrontPriorityQueue().offer(task);
    }

    protected void TaskToTotalQueue(ServiceTask task) {
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }


    @Override
    public void handleData(ServiceTask task) {
        ServiceTask executeTask = null;
        synchronized (this) {
            if (currentTask == null) {
                executeTask = taskQueue.poll();
                if (executeTask != null) {
                    currentTask = executeTask;
                }
            }
            if (currentTask != task) {
                TaskToPriorityQueue(task);
            }
        }

        if (executeTask != null) {
            byte[] data = executeTask.getOrgData();
            this.setPacketId(data[3]);
            this.handleInnerData(data);
        }
    }

    protected abstract void handleInnerData(byte[] data);

    public void writeErrMessage(int vendorCode, String msg) {
        writeErrMessage(vendorCode, "HY000", msg);
    }

    protected void writeErrMessage(int vendorCode, String sqlState, String msg) {
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(nextPacketId());
        err.setErrNo(vendorCode);
        err.setSqlState(StringUtil.encode(sqlState, connection.getCharsetName().getResults()));
        err.setMessage(StringUtil.encode(msg, connection.getCharsetName().getResults()));
        err.write(connection);
    }

    public UserConfig getUserConfig() {
        return userConfig;
    }

    public CharsetNames getCharset() {
        return connection.getCharsetName();
    }
}
