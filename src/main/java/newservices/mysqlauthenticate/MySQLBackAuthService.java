package newservices.mysqlauthenticate;

import newcommon.service.AuthService;
import newcommon.service.ServiceTask;
import newnet.connection.AbstractConnection;
import newservices.mysqlauthenticate.plugin.MySQLAuthPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by szf on 2020/6/19.
 */
public class MySQLBackAuthService  {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackAuthService.class);

    private volatile MySQLAuthPlugin plugin;


}
