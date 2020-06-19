package newservices.mysqlauthenticate.plugin;

/**
 * Created by szf on 2020/6/18.
 */
public class CachingSHA2Pwd implements MySQLAuthPlugin {

    @Override
    public boolean verify() {
        return false;
    }

    @Override
    public void authenticate() {

    }
}
