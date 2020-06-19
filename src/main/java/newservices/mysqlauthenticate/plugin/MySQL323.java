package newservices.mysqlauthenticate.plugin;

/**
 * Created by szf on 2020/6/18.
 */
public class MySQL323 implements MySQLAuthPlugin {
    @Override
    public boolean verify() {
        return false;
    }

    @Override
    public void authenticate() {

    }
}
