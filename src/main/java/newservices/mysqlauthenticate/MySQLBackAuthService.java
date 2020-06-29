package newservices.mysqlauthenticate;

import newservices.mysqlauthenticate.plugin.MySQLAuthPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2020/6/19.
 */
public class MySQLBackAuthService  {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackAuthService.class);

    private volatile MySQLAuthPlugin plugin;


}
