package newcommon.service;

import newnet.AbstractConnection;

import java.io.IOException;

/**
 * Created by szf on 2020/6/19.
 */
public abstract class AuthService extends AbstractService{

    public AuthService(AbstractConnection connection) {
        super(connection);
    }

    public abstract void register() throws IOException;
}
