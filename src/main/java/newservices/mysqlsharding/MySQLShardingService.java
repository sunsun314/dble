package newservices.mysqlsharding;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ChangeUserPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.ServerConnection;
import newcommon.service.AbstractService;
import newcommon.service.ServiceTask;
import newnet.connection.AbstractConnection;

import java.io.IOException;

/**
 * Created by szf on 2020/6/18.
 */
public class MySQLShardingService extends AbstractService {

    MySQLShardingService(AbstractConnection connection) {
        super(connection);
    }

    @Override
    public void handleData(ServiceTask task) {/*
        byte[] data = task.getOrgData();
        ServerConnection sc = (ServerConnection) source;
        sc.startProcess();

        if (isAuthSwitch.compareAndSet(true, false)) {
            sc.changeUserAuthSwitch(data, changeUserPacket);
            return;
        }

        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                sc.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                sc.query(data);
                break;
            case MySQLPacket.COM_PING:
                sc.ping();
                break;
            case MySQLPacket.COM_QUIT:
                sc.close("quit cmd");
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                sc.kill(data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                sc.stmtPrepare(data);
                break;
            case MySQLPacket.COM_STMT_RESET:
                sc.stmtReset(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                sc.stmtExecute(data, blobDataQueue);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                sc.heartbeat(data);
                break;
            case MySQLPacket.COM_SET_OPTION:
                sc.setOption(data);
                break;
            case MySQLPacket.COM_CHANGE_USER:
                changeUserPacket = new ChangeUserPacket(sc.getClientFlags(), CharsetUtil.getCollationIndex(sc.getCharset().getCollation()));
                sc.changeUser(data, changeUserPacket, isAuthSwitch);
                break;
            case MySQLPacket.COM_RESET_CONNECTION:
                sc.resetConnection();
                break;
            default:
                sc.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    */
    }


    @Override
    public void register() throws IOException {

    }
}
