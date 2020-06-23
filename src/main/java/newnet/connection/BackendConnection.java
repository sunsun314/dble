package newnet.connection;

import newcommon.service.AuthResultInfo;
import newnet.SocketWR;

import java.nio.channels.NetworkChannel;

/**
 * Created by szf on 2020/6/23.
 */
public class BackendConnection extends AbstractConnection{

    public BackendConnection(NetworkChannel channel, SocketWR socketWR) {
        super(channel, socketWR);
    }

    @Override
    public void businessClose(String reason) {

    }

    @Override
    public void setConnProperties(AuthResultInfo info) {

    }

    @Override
    public void startFlowControl(com.actiontech.dble.backend.BackendConnection bcon) {

    }

    @Override
    public void stopFlowControl() {

    }

    public void onConnectFailed(Throwable e){

    }
}
