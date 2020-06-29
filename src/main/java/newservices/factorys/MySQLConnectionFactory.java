package newservices.factorys;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import newcommon.service.AbstractService;
import newnet.factory.PooledConnectionFactory;

/**
 * Created by szf on 2020/6/29.
 */
public class MySQLConnectionFactory extends PooledConnectionFactory {

    @Override
    public BackendConnection make(PhysicalDbInstance pool, AbstractService nextService, String schema) {
        return null;
    }
}
