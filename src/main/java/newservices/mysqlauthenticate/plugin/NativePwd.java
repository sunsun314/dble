package newservices.mysqlauthenticate.plugin;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.RandomUtil;
import newcommon.proto.mysql.packet.HandshakeV10Packet;
import newcommon.service.AuthResultInfo;
import newnet.connection.AbstractConnection;
import newservices.mysqlauthenticate.PluginName;

/**
 * Created by szf on 2020/6/18.
 */
public class NativePwd extends MySQLAuthPlugin {

    protected byte[] seed;
    AbstractConnection connection;

    public NativePwd(AbstractConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean verify() {
        return false;
    }

    @Override
    public PluginName handleData(byte[] data) {
        return null;
    }

    @Override
    public void handleSwitchData(byte[] data) {

    }

    @Override
    public byte[] greeting() {
        // generate auth data
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        // save  auth data
        byte[] rand = new byte[rand1.length + rand2.length];
        System.arraycopy(rand1, 0, rand, 0, rand1.length);
        System.arraycopy(rand2, 0, rand, rand1.length, rand2.length);
        this.seed = rand;

        HandshakeV10Packet hs = new HandshakeV10Packet();
        hs.setPacketId(0);
        hs.setProtocolVersion(Versions.PROTOCOL_VERSION);  // [0a] protocol version   V10
        hs.setServerVersion(Versions.getServerVersion());
        hs.setThreadId(connection.getId());
        hs.setSeed(rand1);
        hs.setServerCapabilities(getServerCapabilities());
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
        hs.setServerCharsetIndex((byte) (charsetIndex & 0xff));
        hs.setServerStatus(2);
        hs.setRestOfScrambleBuff(rand2);

        //write out
        hs.write(connection);
        return seed;
    }

    @Override
    public PluginName getName() {
        return null;
    }

    @Override
    public AuthResultInfo getInfo() {
        return null;
    }
}
