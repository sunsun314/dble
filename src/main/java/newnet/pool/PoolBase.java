package newnet.pool;

import com.actiontech.dble.config.model.db.DbInstanceConfig;
import newcommon.service.AbstractService;
import newnet.connection.PooledConnection;
import newnet.factory.PooledConnectionFactory;

import java.io.IOException;

public class PoolBase {

    protected final DbInstanceConfig config;
    protected final ReadTimeStatusInstance instance;
    protected final PooledConnectionFactory factory;

    public PoolBase(DbInstanceConfig dbConfig, ReadTimeStatusInstance instance, PooledConnectionFactory factory) {
        this.config = dbConfig;
        this.instance = instance;
        this.factory = factory;
    }

    /**
     * only for heartbeat
     * @return
     */
    public void newConnection(String schema, AbstractService service) {
        try {
            factory.make(instance, service, schema);
        } catch (IOException ioe) {
            // todo response for IOException  service.connectionError(ioe, null);
            ioe.printStackTrace();
        }
    }

    PooledConnection newConnection(String schema, PooledConnectionListener listener) {
        PooledConnection conn = null;
        try {
            return  factory.make(instance, listener, schema);
        } catch (IOException ioe) {
            listener.onCreateFail(conn, ioe);
            return null;
        }
    }
}
