package newservices.mysqlsharding;

import newcommon.service.AuthResultInfo;
import newcommon.service.ServiceTask;
import newnet.connection.AbstractConnection;
import newservices.MySQLBasedService;


/**
 * Created by szf on 2020/6/18.
 */
public class MySQLShardingService extends MySQLBasedService {

    public MySQLShardingService(AbstractConnection connection) {
        super(connection);
    }

    @Override
    protected void handleInnerData(byte[] data) {

    }


    public void initFromAuthInfo(AuthResultInfo info) {
      /*  this.userConfig = info.getUserConfig();
        ServerConnection sc = (ServerConnection) source;
        sc.setUserConfig((ServerUserConfig) userConfig);
        sc.setUser(user);
        if (userConfig instanceof ShardingUserConfig) {
            sc.setQueryHandler(new ServerQueryHandler(sc));
            sc.setLoadDataInfileHandler(new ServerLoadDataInfileHandler(sc));
            sc.setPrepareHandler(new ServerPrepareHandler(sc));
            SystemConfig sys = SystemConfig.getInstance();
            sc.setTxIsolation(sys.getTxIsolation());
            sc.setSession2(new NonBlockingSession(sc));
            sc.getSession2().setRowCount(0);
            sc.setAuthenticated(true);
            sc.setSchema(auth.getDatabase());
            sc.initCharsetIndex(auth.getCharsetIndex());
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
*/
    }
}
