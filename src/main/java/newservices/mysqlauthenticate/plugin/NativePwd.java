package newservices.mysqlauthenticate.plugin;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.mysql.BinaryPacket;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.RandomUtil;
import newbootstrap.DbleServer;
import newcommon.proto.mysql.packet.AuthPacket;
import newcommon.proto.mysql.packet.AuthSwitchRequestPackage;
import newcommon.proto.mysql.packet.AuthSwitchResponsePackage;
import newcommon.proto.mysql.packet.ErrorPacket;
import newcommon.proto.mysql.packet.HandshakeV10Packet;
import newcommon.proto.mysql.packet.OkPacket;
import newcommon.service.AuthResultInfo;
import newnet.connection.AbstractConnection;
import newservices.mysqlauthenticate.PasswordAuthPlugin;
import newservices.mysqlauthenticate.PluginName;
import newservices.mysqlauthenticate.util.AuthUtil;

import java.security.NoSuchAlgorithmException;

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
    public void authenticate(String user, String password, String schema, byte packetId) {
        AuthPacket packet = new AuthPacket();
        packet.setPacketId(packetId);
        packet.setMaxPacketSize(connection.getMaxPacketSize());
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
        packet.setCharsetIndex(charsetIndex);
        packet.setUser(user);
        try {
            sendAuthPacket(packet, PasswordAuthPlugin.passwd(password, handshakePacket), PLUGIN_NAME.name(), schema);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public PluginName handleBackData(byte[] data) throws Exception {
        switch (data[4]) {
            case AuthSwitchRequestPackage.STATUS:
                BinaryPacket bin2 = new BinaryPacket();
                String authPluginName = bin2.getAuthPluginName(data);
                byte[] authPluginData = bin2.getAuthPluginData(data);
                try {
                    PluginName name = PluginName.valueOf(authPluginName);
                    return name;
                } catch (IllegalArgumentException e) {
                    return PluginName.unsupport_plugin;
                }
            case OkPacket.FIELD_COUNT:
                // execute auth response
                info = new AuthResultInfo(null, handshakePacket);
                return PluginName.plugin_same_with_default;
            case ErrorPacket.FIELD_COUNT:
                ErrorPacket err = new ErrorPacket();
                err.read(data);
                String errMsg = new String(err.getMessage());
                info = new AuthResultInfo(errMsg, handshakePacket);
                return PluginName.plugin_same_with_default;
            default:
                return PluginName.unsupport_plugin;
        }
    }


    @Override
    public PluginName handleData(byte[] data) {
        AuthPacket auth = new AuthPacket();
        auth.read(data);
        authPacket = auth;
        try {
            PluginName name = PluginName.valueOf(auth.getAuthPlugin());
            if (PLUGIN_NAME == name) {
                String errMsg = AuthUtil.auhth(new UserName(authPacket.getUser(), authPacket.getTenant()), connection, seed, authPacket.getPassword(), authPacket.getDatabase(), PLUGIN_NAME);
                UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(new UserName(authPacket.getUser(), authPacket.getTenant()));
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

        String errMsg = AuthUtil.auhth(new UserName(authPacket.getUser(), authPacket.getTenant()), connection, seed, authPacket.getPassword(), authPacket.getDatabase(), PLUGIN_NAME);

        UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(new UserName(authPacket.getUser(), authPacket.getTenant()));
        info = new AuthResultInfo(errMsg, authPacket, userConfig);
    }


    @Override
    public PluginName getName() {
        return null;
    }

}
