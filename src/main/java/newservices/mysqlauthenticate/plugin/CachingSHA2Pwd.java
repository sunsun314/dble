package newservices.mysqlauthenticate.plugin;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.singleton.FrontendUserManager;
import newcommon.proto.mysql.packet.*;
import newcommon.proto.mysql.packet.AuthPacket;
import newcommon.proto.mysql.packet.AuthSwitchRequestPackage;
import newcommon.proto.mysql.packet.AuthSwitchResponsePackage;
import newcommon.proto.mysql.packet.BinaryPacket;
import newcommon.proto.mysql.packet.ErrorPacket;
import newcommon.proto.mysql.packet.OkPacket;
import newcommon.service.AuthResultInfo;
import newnet.connection.AbstractConnection;
import newservices.mysqlauthenticate.PasswordAuthPlugin;
import newservices.mysqlauthenticate.PluginName;
import newservices.mysqlauthenticate.util.AuthUtil;
import newservices.mysqlsharding.backend.CharsetUtil;

import java.security.NoSuchAlgorithmException;

import static newservices.mysqlauthenticate.PasswordAuthPlugin.AUTH_SWITCH_MORE;
import static newservices.mysqlauthenticate.PasswordAuthPlugin.GETPUBLICKEY;
import static newservices.mysqlauthenticate.PluginName.caching_sha2_password;
import static newservices.mysqlauthenticate.PluginName.plugin_same_with_default;

/**
 * Created by szf on 2020/6/18.
 */
public class CachingSHA2Pwd extends MySQLAuthPlugin {

    private final PluginName PLUGIN_NAME = caching_sha2_password;

    private static final int STAGE_QUICK_AUTH = 1;
    private static final int WATI_FOR_PUBLIC_KEY = 2;
    private static final int WATI_FOR_AUTH_RESULT = 3;

    private volatile int clientAuthStage = STAGE_QUICK_AUTH;

    private volatile byte[] authPluginData;

    private volatile String password;

    public CachingSHA2Pwd(AbstractConnection connection) {
        super(connection);
    }

    public CachingSHA2Pwd(MySQLAuthPlugin plugin) {
        super(plugin);
    }

    @Override
    public void authenticate(String user, String password, String schema, byte packetId) {
        AuthPacket packet = new AuthPacket();
        this.password = password;
        packet.setPacketId(packetId);
        packet.setMaxPacketSize(connection.getMaxPacketSize());
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
        packet.setCharsetIndex(charsetIndex);
        packet.setUser(user);
        try {
            int sl1 = handshakePacket.getSeed().length;
            int sl2 = handshakePacket.getRestOfScrambleBuff().length;
            seed = new byte[sl1 + sl2];
            System.arraycopy(handshakePacket.getSeed(), 0, seed, 0, sl1);
            System.arraycopy(handshakePacket.getRestOfScrambleBuff(), 0, seed, sl1, sl2);

            sendAuthPacket(packet, PasswordAuthPlugin.passwdSha256(password, handshakePacket), PLUGIN_NAME.name(), schema);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
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
    public PluginName handleBackData(byte[] data) throws Exception {
        if (clientAuthStage == WATI_FOR_PUBLIC_KEY) {
            if (checkPubicKey(data)) {
                // get the public from the mysql, use the public key to send the new auth pass
                BinaryPacket binPacket = new BinaryPacket();
                byte[] publicKey = binPacket.readKey(data);
                byte[] authResponse = PasswordAuthPlugin.sendEnPasswordWithPublicKey(authPluginData == null ? seed : authPluginData, publicKey, password, ++data[3]);
                connection.write(authResponse);
                this.clientAuthStage = WATI_FOR_PUBLIC_KEY;
                return plugin_same_with_default;
            }
        }
        switch (data[4]) {
            case AUTH_SWITCH_MORE:
                //need full auth of the caching_sha2_password
                this.clientAuthStage = WATI_FOR_PUBLIC_KEY;
                BinaryPacket binaryPacket = new BinaryPacket();
                binaryPacket.setData(new byte[]{2});
                binaryPacket.setPacketId(++data[3]);
                binaryPacket.write(connection);
                return plugin_same_with_default;
            case OkPacket.FIELD_COUNT:
                // get ok from mysql,login success
                info = new AuthResultInfo(null, handshakePacket);
                return PluginName.plugin_same_with_default;
            case ErrorPacket.FIELD_COUNT:
                // get error response from the mysql,login be rejected
                ErrorPacket err = new ErrorPacket();
                err.read(data);
                String errMsg = new String(err.getMessage());
                info = new AuthResultInfo(errMsg, handshakePacket);
                return PluginName.plugin_same_with_default;
            case AuthSwitchRequestPackage.STATUS:
                //need auth swith for other plugin
                BinaryPacket bin2 = new BinaryPacket();
                String authPluginName = bin2.getAuthPluginName(data);
                authPluginData = bin2.getAuthPluginData(data);
                try {
                    PluginName name = PluginName.valueOf(authPluginName);
                    return name;
                } catch (IllegalArgumentException e) {
                    return PluginName.unsupport_plugin;
                }
        }
        return PluginName.unsupport_plugin;
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


    public boolean checkPubicKey(byte[] data) {
        return data[0] == (byte) 0xc4 && data[1] == (byte) 1;
    }
}
