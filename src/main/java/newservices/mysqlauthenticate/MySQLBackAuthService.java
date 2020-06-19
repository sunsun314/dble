package newservices.mysqlauthenticate;

import newcommon.service.AbstractService;
import newcommon.service.AuthService;
import newcommon.service.ServiceTask;
import newnet.AbstractConnection;
import newservices.mysqlauthenticate.plugin.MySQLAuthPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by szf on 2020/6/19.
 */
public class MySQLBackAuthService extends AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackAuthService.class);

    private volatile MySQLAuthPlugin plugin;


    public MySQLBackAuthService(AbstractConnection connection) {
        super(connection);
    }

    @Override
    public void register() throws IOException {

    }

    @Override
    public void handleData(ServiceTask task) {
        //原则是大部分数据应该交由插件处理

        //并且这里可以只处理关于前端验证的故事


    }
}
