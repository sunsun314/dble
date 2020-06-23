/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package newservices.mysqlsharding.factory;


import newnet.SocketWR;
import newnet.connection.FrontendConnection;
import newnet.factory.FrontendConnectionFactory;

import java.io.IOException;
import java.nio.channels.NetworkChannel;


public class ServerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(NetworkChannel channel, SocketWR socketWR) throws IOException {
        FrontendConnection c = new FrontendConnection(channel, socketWR);
        return c;
    }

}
