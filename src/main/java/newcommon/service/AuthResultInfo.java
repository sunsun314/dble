package newcommon.service;

import com.actiontech.dble.config.model.user.UserConfig;
import newcommon.proto.mysql.packet.AuthPacket;

/**
 * Created by szf on 2020/6/19.
 */
public class AuthResultInfo {

    private boolean success;
    private String errorMsg;
    private UserConfig userConfig;

    private AuthPacket mysqlAuthPacket = null;

    public AuthResultInfo(String errorMsg, AuthPacket authPacket, UserConfig userConfig) {
        this.success = errorMsg == null ? true : false;
        this.errorMsg = errorMsg;
        this.userConfig = userConfig;
        this.mysqlAuthPacket = authPacket;
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
