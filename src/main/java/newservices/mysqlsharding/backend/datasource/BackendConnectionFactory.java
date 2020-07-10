package newservices.mysqlsharding.backend.datasource;

import newcommon.service.AbstractService;
import newnet.connection.PooledConnection;
import newnet.factory.PooledConnectionFactory;
import newnet.pool.PooledConnectionListener;
import newnet.pool.ReadTimeStatusInstance;

import java.io.IOException;

/**
 * Created by szf on 2020/6/30.
 */
public class BackendConnectionFactory extends PooledConnectionFactory {
    @Override
    public PooledConnection make(ReadTimeStatusInstance instance, AbstractService nextService, String schema) throws IOException {
        return null;
    }

    @Override
    public PooledConnection make(ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) throws IOException {
        return null;
    }
}
