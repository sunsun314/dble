/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package newnet.factory;

import com.actiontech.dble.DbleServer;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

/**
 * @author mycat
 */
public abstract class BackendConnectionFactory {

    protected NetworkChannel openSocketChannel(boolean isAIO)
            throws IOException {
        if (isAIO) {
            return AsynchronousSocketChannel.open(DbleServer.getInstance().getNextAsyncChannelGroup());
        } else {
            SocketChannel channel = null;
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            return channel;
        }

    }

}
