package newservices.mysqlauthenticate.plugin;

import com.actiontech.dble.config.model.SystemConfig;
import newcommon.proto.mysql.packet.HandshakeV10Packet;
import newnet.connection.AbstractConnection;
import newnet.connection.BackendConnection;
import newservices.mysqlauthenticate.PluginName;

import static newservices.mysqlauthenticate.PluginName.unsupport_plugin;

/**
 * Created by szf on 2020/6/30.
 */
public class BackendDefaulPlugin extends MySQLAuthPlugin {

    public BackendDefaulPlugin(AbstractConnection connection) {
        super(connection);
    }

    @Override
    public void authenticate(String user, String password, String schema) {

    }

    @Override
    public PluginName handleData(byte[] data) {
        return null;
    }

    @Override
    public PluginName handleBackData(byte[] data) {
        handshakePacket = new HandshakeV10Packet();
        handshakePacket.read(data);

        ((BackendConnection) connection).setThreadId(handshakePacket.getThreadId());
        connection.initCharacterSet(SystemConfig.getInstance().getCharset());

        String authPluginName = new String(handshakePacket.getAuthPluginName());
        try {
            PluginName name = PluginName.valueOf(authPluginName);
            return name;
        } catch (IllegalArgumentException e) {
            return PluginName.unsupport_plugin;
        }
    }


    @Override
    public void handleSwitchData(byte[] data) {

    }

    @Override
    public byte[] greeting() {
        return new byte[0];
    }

    @Override
    public PluginName getName() {
        return unsupport_plugin;
    }
}
