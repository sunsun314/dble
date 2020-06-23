package newservices.mysqlauthenticate;

import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import newcommon.proto.mysql.packet.*;
import newcommon.service.AuthResultInfo;
import newcommon.service.AuthService;
import newcommon.service.ServiceTask;
import newnet.connection.AbstractConnection;
import newservices.mysqlauthenticate.plugin.CachingSHA2Pwd;
import newservices.mysqlauthenticate.plugin.MySQLAuthPlugin;
import newservices.mysqlauthenticate.plugin.NativePwd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static newservices.mysqlauthenticate.PluginName.caching_sha2_password;
import static newservices.mysqlauthenticate.PluginName.mysql_native_password;

/**
 * Created by szf on 2020/6/18.
 */
public class MySQLFrontAuthService extends AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLFrontAuthService.class);
    private static final byte[] AUTH_OK = new byte[]{7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};
    private static final byte[] SWITCH_AUTH_OK = new byte[]{7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0};

    private volatile MySQLAuthPlugin plugin;

    private volatile byte[] seed;

    private volatile boolean hasAuthSwitched;

    public MySQLFrontAuthService(AbstractConnection connection) {
        super(connection);
        SystemConfig.getInstance().getFakeMySQLVersion();
        //如果是5.7 或者之前的版本，就是用NATIVE的插件
        plugin = new NativePwd(connection);
    }

    @Override
    public void register() throws IOException {
        //让验证插件自己 写出握手包
        seed = plugin.greeting();
    }

    @Override
    public void handleData(ServiceTask task) {
        byte[] data = task.getOrgData();
        if (data.length == QuitPacket.QUIT.length && data[4] == MySQLPacket.COM_QUIT) {
            connection.close("quit packet");
        } else if (data.length == PingPacket.PING.length && data[4] == PingPacket.COM_PING) {
            pingResponse();
        } else {
            if (hasAuthSwitched) {
                //if got the switch response,check the result
                plugin.handleSwitchData(data);
                checkForResult(plugin.getInfo());
            } else {
                switch (plugin.handleData(data)) {
                    case caching_sha2_password:
                        hasAuthSwitched = true;
                        requestToSwitch(caching_sha2_password);
                        break;
                    case mysql_native_password:
                        hasAuthSwitched = true;
                        requestToSwitch(mysql_native_password);
                        break;
                    case plugin_same_with_default:
                        checkForResult(plugin.getInfo());
                        break;
                    default:
                        //try to switch plugin to the default
                        requestToSwitch(plugin.getName());
                }
            }
        }
    }

    private void checkForResult(AuthResultInfo info) {
        if (info.isSuccess()) {
            String errMsg = checkUserRights(info.getUserConfig());
            if (errMsg != null) {
                writeOutErrorMessage(errMsg);
            } else {
                connection.setConnProperties(info);
                MySQLPacket packet = new OkPacket();
                packet.write(connection);
            }
        } else {
            writeOutErrorMessage(info.getErrorMsg());
        }
    }

    private void writeOutErrorMessage(String errorMsg) {
        this.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, errorMsg);
    }

    private void requestToSwitch(PluginName name) {
        AuthSwitchRequestPackage authSwitch = new AuthSwitchRequestPackage(name.toString().getBytes(), seed);
        authSwitch.setPacketId(this.nextPacketId());
        authSwitch.write(connection);
    }

    private boolean isPluginSupported(String authPlugin) {
        return true;
    }


    private String checkUserRights(UserConfig config) {
        return null;
    }

    private void pingResponse() {

    }

    protected int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (usingCompress) {
            flag |= Capabilities.CLIENT_COMPRESS;
        }

        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        flag |= Capabilities.CLIENT_CONNECT_ATTRS;
        return flag;
    }


    public boolean checkPubicKey(byte[] data) {
        return data[0] == (byte) 0xc4 && data[1] == (byte) 1 && data[2] == (byte) 0 && (data[3] == (byte) 4 || data[3] == (byte) 6);
    }

    public MySQLAuthPlugin getPlugin(byte[] data) throws Exception {
        BinaryPacket bin2 = new BinaryPacket();
        String authPluginName = bin2.getAuthPluginName(data);
        byte[] authPluginData = bin2.getAuthPluginData(data);
        if (authPluginName.equals(new String(HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN))) {
            return new NativePwd(this.connection);
        } else if (authPluginName.equals(new String(HandshakeV10Packet.CACHING_SHA2_PASSWORD_PLUGIN))) {
            return new CachingSHA2Pwd();
        }
        return null;
    }
}
