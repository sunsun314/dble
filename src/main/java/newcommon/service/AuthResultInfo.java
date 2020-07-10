package newcommon.service;

import com.actiontech.dble.config.model.user.UserConfig;
import newcommon.proto.mysql.packet.AuthPacket;
import newcommon.proto.mysql.packet.HandshakeV10Packet;

/**
 * Created by szf on 2020/6/19.
 */
public class AuthResultInfo {

    private boolean success;
    private String errorMsg;
    private UserConfig userConfig;

    private AuthPacket mysqlAuthPacket = null;
    private HandshakeV10Packet handshakePacket = null;

    public AuthResultInfo(String errorMsg, AuthPacket authPacket, UserConfig userConfig) {
        this.success = errorMsg == null ? true : false;
        this.errorMsg = errorMsg;
        this.userConfig = userConfig;
        this.mysqlAuthPacket = authPacket;
    }

    public AuthResultInfo(String errorMsg, HandshakeV10Packet handshakePacket) {
        this.success = errorMsg == null ? true : false;
        this.handshakePacket = handshakePacket;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMsg() {
        return errorMsg;
    }


    public UserConfig getUserConfig() {
        return userConfig;
    }

    public AuthPacket getMysqlAuthPacket() {
        return mysqlAuthPacket;
    }
}
