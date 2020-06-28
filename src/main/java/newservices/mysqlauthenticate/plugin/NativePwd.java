package newservices.mysqlauthenticate.plugin;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.RandomUtil;
import newbootstrap.DbleServer;
import newcommon.proto.mysql.packet.AuthPacket;
import newcommon.proto.mysql.packet.AuthSwitchResponsePackage;
import newcommon.proto.mysql.packet.HandshakeV10Packet;
import newcommon.service.AuthResultInfo;
import newnet.connection.AbstractConnection;
import newservices.mysqlauthenticate.PluginName;
import newservices.mysqlauthenticate.util.AuthUtil;

import static newservices.mysqlauthenticate.PluginName.mysql_native_password;

/**
 * Created by szf on 2020/6/18.
 */
public class NativePwd extends MySQLAuthPlugin {

    private final PluginName PLUGIN_NAME = mysql_native_password;

    public NativePwd(AbstractConnection connection) {
        super(connection);
    }

    public NativePwd(MySQLAuthPlugin plugin) {
        super(plugin);
    }

    @Override
    public boolean verify() {
        return false;
    }

    @Override
    public PluginName handleData(byte[] data) {
        AuthPacket auth = new AuthPacket();
        auth.read(data);
        authPacket = auth;
        try {
            PluginName name = PluginName.valueOf(auth.getAuthPlugin());
            if (PLUGIN_NAME == name) {
                String errMsg = AuthUtil.auhth(new Pair<>(authPacket.getUser(), authPacket.getTenant()), connection, seed, authPacket.getPassword(), authPacket.getDatabase());
                UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(new Pair<>(authPacket.getUser(), authPacket.getTenant()));
                info = new AuthResultInfo(errMsg, authPacket, userConfig);
                return PluginName.plugin_same_with_default;
            } else {
                return name;
            }
        } catch (IllegalArgumentException e) {
            return PluginName.unsupport_plugin;
        }
    }

    @Override
    public void handleSwitchData(byte[] data) {
        AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
        authSwitchResponse.read(data);
        authPacket.setPassword(authSwitchResponse.getAuthPluginData());

        String errMsg = AuthUtil.auhth(new Pair<>(authPacket.getUser(), authPacket.getTenant()), connection, seed, authPacket.getPassword(), authPacket.getDatabase());

        UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(new Pair<>(authPacket.getUser(), authPacket.getTenant()));
        info = new AuthResultInfo(errMsg, authPacket, userConfig);

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

}
