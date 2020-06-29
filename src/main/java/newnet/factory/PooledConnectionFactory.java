/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package newnet.factory;

import newcommon.service.AbstractService;
import newnet.connection.PooledConnection;
import newnet.pool.PooledConnectionListener;
import newnet.pool.ReadTimeStatusInstance;

import java.io.IOException;


public abstract class PooledConnectionFactory {

    public abstract PooledConnection make(ReadTimeStatusInstance instance, AbstractService nextService, String schema) throws IOException;

    public abstract PooledConnection make(ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) throws IOException;
}
