package newservices.mysqlauthenticate.plugin;

import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.SystemConfig;
import newcommon.proto.mysql.packet.AuthPacket;
import newcommon.service.AuthResultInfo;
import newnet.connection.AbstractConnection;
import newservices.mysqlauthenticate.PluginName;


/**
 * Created by szf on 2020/6/18.
 */
public abstract class MySQLAuthPlugin {

    protected byte[] seed;
    private boolean authSwitch;
    protected final AbstractConnection connection;
    protected AuthResultInfo info;
    protected AuthPacket authPacket;

    MySQLAuthPlugin(AbstractConnection connection) {
        this.connection = connection;
    }

    public MySQLAuthPlugin(MySQLAuthPlugin plugin){
        this.seed  = plugin.seed;
        this.authPacket = plugin.authPacket;
        this.connection  = plugin.connection;
    }


    public abstract boolean verify();

    public abstract PluginName handleData(byte[] data);

    public abstract void handleSwitchData(byte[] data);

    public abstract byte[] greeting();

    public abstract PluginName getName();

    public AuthResultInfo getInfo(){
        return info;
    }

    public byte[] getSeed() {
        return seed;
    }

    public void switchNotified() {
        authSwitch = true;
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

}
