package newservices;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.StringUtil;
import newcommon.proto.mysql.packet.ErrorPacket;
import newcommon.service.AbstractService;
import newcommon.service.AuthResultInfo;
import newcommon.service.ServiceTask;
import newnet.connection.AbstractConnection;

/**
 * Created by szf on 2020/6/28.
 */
public abstract class MySQLBasedService extends AbstractService {

    protected UserConfig userConfig;

    protected Pair<String, String> user;

    protected long clientFlags;

    protected volatile CharsetNames charsetName = new CharsetNames();

    public MySQLBasedService(AbstractConnection connection) {
        super(connection);
    }

    public abstract void initFromAuthInfo(AuthResultInfo info);

    public void initCharsetIndex(int ci) {
        String name = CharsetUtil.getCharset(ci);
        if (name != null) {
            charsetName.setClient(name);
            charsetName.setResults(name);
            charsetName.setCollation(CharsetUtil.getDefaultCollation(name));
        }
    }

    @Override
    public void handleData(ServiceTask task) {
        byte[] data = task.getOrgData();
        this.setPacketId(data[3]);
        //todo:这里需要处理对应的高优先级回环的问题，之后再来进行处理
        this.handleInnerData(data);
    }

    protected abstract void handleInnerData(byte[] data);

    public void writeErrMessage(int vendorCode, String msg) {
        writeErrMessage(vendorCode, "HY000", msg);
    }

    protected void writeErrMessage(int vendorCode, String sqlState, String msg) {
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(nextPacketId());
        err.setErrNo(vendorCode);
        err.setSqlState(StringUtil.encode(sqlState, charsetName.getResults()));
        err.setMessage(StringUtil.encode(msg, charsetName.getResults()));
        err.write(connection);
    }

    public UserConfig getUserConfig() {
        return userConfig;
    }

    public CharsetNames getCharset() {
        return charsetName;
    }
}
