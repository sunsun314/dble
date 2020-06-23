package newnet.connection;

import com.actiontech.dble.backend.BackendConnection;
import newcommon.service.AuthResultInfo;
import newnet.SocketWR;

import java.nio.channels.NetworkChannel;

/**
 * Created by szf on 2020/6/23.
 */
public class FrontendConnection extends AbstractConnection{

    public FrontendConnection(NetworkChannel channel, SocketWR socketWR) {
        super(channel, socketWR);
    }

    @Override
    public void businessClose(String reason) {

    }

    @Override
    public void setConnProperties(AuthResultInfo info) {

    }

    @Override
    public void startFlowControl(BackendConnection bcon) {

    }

    @Override
    public void stopFlowControl() {

    }
}
