package newcommon.service;

import com.actiontech.dble.config.model.user.UserConfig;

/**
 * Created by szf on 2020/6/19.
 */
public class AuthResultInfo {

    private boolean success;
    private String errorMsg;
    private UserConfig userConfig;

    AuthResultInfo(String errorMsg) {
        this.success = false;
        this.errorMsg = errorMsg;
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
}
