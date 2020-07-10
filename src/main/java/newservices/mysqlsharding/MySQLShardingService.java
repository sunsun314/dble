package newservices.mysqlsharding;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.handler.ServerLoadDataInfileHandler;
import com.actiontech.dble.server.handler.ServerPrepareHandler;
import newcommon.proto.mysql.packet.AuthPacket;
import newcommon.proto.mysql.packet.MySQLPacket;
import newcommon.service.AuthResultInfo;
import newnet.connection.AbstractConnection;
import newservices.MySQLBasedService;


/**
 * Created by szf on 2020/6/18.
 */
public class MySQLShardingService extends MySQLBasedService {

    private final ServerQueryHandler handler;

    private final ServerLoadDataInfileHandler loadDataInfileHandler;

    private final FrontendPrepareHandler prepareHandler;

    protected UserName user;

    private volatile int txIsolation;

    protected boolean isAuthenticated;

    protected String schema;


    private final NonBlockingSession session;

    public MySQLShardingService(AbstractConnection connection) {
        super(connection);
        this.handler = new ServerQueryHandler(this);
        this.loadDataInfileHandler  = new ServerLoadDataInfileHandler(this);
        this.prepareHandler = new ServerPrepareHandler(this);
        this.session = new NonBlockingSession(this);
    }

    @Override
    protected void handleInnerData(byte[] data) {
        ServerConnection sc = (ServerConnection) source;
        sc.startProcess();

        if (isAuthSwitch.compareAndSet(true, false)) {
            commands.doOther();
            sc.changeUserAuthSwitch(data, changeUserPacket);
            return;
        }
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                sc.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                sc.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                sc.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                sc.close("quit cmd");
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                commands.doKill();
                sc.kill(data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                sc.stmtPrepare(data);
                break;
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                sc.stmtReset(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                sc.stmtExecute(data, blobDataQueue);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                sc.heartbeat(data);
                break;
            case MySQLPacket.COM_SET_OPTION:
                commands.doOther();
                sc.setOption(data);
                break;
            case MySQLPacket.COM_CHANGE_USER:
                commands.doOther();
                changeUserPacket = new ChangeUserPacket(sc.getClientFlags(), CharsetUtil.getCollationIndex(sc.getCharset().getCollation()));
                sc.changeUser(data, changeUserPacket, isAuthSwitch);
                break;
            case MySQLPacket.COM_RESET_CONNECTION:
                commands.doOther();
                sc.resetConnection();
                break;
            default:
                commands.doOther();
                sc.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }


    public void initFromAuthInfo(AuthResultInfo info) {

        AuthPacket auth = info.getMysqlAuthPacket();

        //先设定用户
        this.userConfig = info.getUserConfig();
        this.user = new UserName(auth.getUser(), auth.getTenant());
        //然后设定各种handler

        //获取到面向前端连接的的属性，并且进行初始化

        //打印一行日志

        SystemConfig sys = SystemConfig.getInstance();
        txIsolation = sys.getTxIsolation();

        this.initCharsetIndex(auth.getCharsetIndex());
        sc.setHandler(new ShardingUserCommandHandler(sc));
        sc.setMultStatementAllow(auth.isMultStatementAllow());
        sc.setClientFlags(auth.getClientFlags());
        boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.getClientFlags());
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (clientCompress && usingCompress) {
            sc.setSupportCompress(true);
        }
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(sc).append('\'').append(auth.getUser()).append("' login success");
            byte[] extra = auth.getExtra();
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.debug(s.toString());
        }
    }
}
