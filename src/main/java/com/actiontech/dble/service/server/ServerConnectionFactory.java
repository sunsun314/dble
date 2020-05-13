/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.server;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.common.config.ServerPrivileges;
import com.actiontech.dble.common.config.model.SystemConfig;
import com.actiontech.dble.common.net.FrontendConnection;
import com.actiontech.dble.common.net.factory.FrontendConnectionFactory;
import com.actiontech.dble.service.server.handler.loaddata.ServerLoadDataInfileHandler;
import com.actiontech.dble.service.server.handler.ServerPrepareHandler;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

/**
 * @author mycat
 */
public class ServerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(NetworkChannel channel) throws IOException {
        ServerConnection c = new ServerConnection(channel);
        c.setSocketParams(true);
        c.setPrivileges(ServerPrivileges.instance());
        c.setQueryHandler(new ServerQueryHandler(c));
        c.setLoadDataInfileHandler(new ServerLoadDataInfileHandler(c));
        c.setPrepareHandler(new ServerPrepareHandler(c));
        SystemConfig sys = DbleServer.getInstance().getConfig().getSystem();
        c.setTxIsolation(sys.getTxIsolation());
        c.setSession2(new NonBlockingSession(c));
        return c;
    }

}
