/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/

import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.factory.FrontendConnectionFactory;
import newnet.connection.FrontendConnection;
import newnet.impl.nio.NIOSocketWR;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

public class ManagerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(NetworkChannel channel, SocketWR ) throws IOException {
        FrontendConnection c = new FrontendConnection(channel,new NIOSocketWR());
        return c;
    }


}
