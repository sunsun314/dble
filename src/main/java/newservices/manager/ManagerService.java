package newservices.manager;

import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.route.parser.util.Pair;
import newcommon.service.AuthResultInfo;
import newnet.connection.AbstractConnection;
import newservices.MySQLBasedService;

/**
 * Created by szf on 2020/6/28.
 */
public class ManagerService extends MySQLBasedService {

    private final ManagerQueryHandler handler;

    public ManagerService(AbstractConnection connection) {
        super(connection);
        this.handler = new ManagerQueryHandler(this);
    }

    @Override
    public void initFromAuthInfo(AuthResultInfo info) {
        this.user = new Pair<>(info.getMysqlAuthPacket().getUser(), "");
        this.userConfig = info.getUserConfig();
        initCharsetIndex(info.getMysqlAuthPacket().getCharsetIndex());
        this.clientFlags = info.getMysqlAuthPacket().getClientFlags();
    }


    @Override
    protected void handleInnerData(byte[] data) {
        //todo real business for manager user

    }

    public ManagerUserConfig getUserConfig() {
        return (ManagerUserConfig)userConfig;
    }
}
