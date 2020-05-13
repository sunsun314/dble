/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.manager;

import com.actiontech.dble.common.net.FrontendConnection;
import com.actiontech.dble.common.net.factory.FrontendConnectionFactory;
import com.actiontech.dble.service.handler.ManagerAuthenticator;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

//import MycatPrivileges;

/**
 * @author mycat
 */
public class ManagerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(NetworkChannel channel) throws IOException {
        ManagerConnection c = new ManagerConnection(channel);
        c.setSocketParams(true);
        c.setPrivileges(ManagerPrivileges.instance());
        c.setHandler(new ManagerAuthenticator(c));
        c.setQueryHandler(new ManagerQueryHandler(c));
        return c;
    }

}
