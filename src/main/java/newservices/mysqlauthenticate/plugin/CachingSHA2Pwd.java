package newservices.mysqlauthenticate.plugin;

import newcommon.service.AuthResultInfo;
import newservices.mysqlauthenticate.PluginName;

/**
 * Created by szf on 2020/6/18.
 */
public class CachingSHA2Pwd extends MySQLAuthPlugin {


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
        return new byte[0];
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
