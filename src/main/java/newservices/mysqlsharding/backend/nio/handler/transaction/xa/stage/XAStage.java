package newservices.mysqlsharding.backend.nio.handler.transaction.xa.stage;

import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import newservices.mysqlsharding.backend.nio.MySQLConnection;
import newservices.mysqlsharding.backend.nio.handler.transaction.ImplicitCommitHandler;
import newservices.mysqlsharding.backend.nio.handler.transaction.TransactionStage;
import newservices.mysqlsharding.backend.nio.handler.transaction.xa.handler.AbstractXAHandler;

public abstract class XAStage implements TransactionStage {

    public static final String END_STAGE = "XA END STAGE";
    public static final String PREPARE_STAGE = "XA PREPARE STAGE";
    public static final String COMMIT_STAGE = "XA COMMIT STAGE";
    public static final String COMMIT_FAIL_STAGE = "XA COMMIT FAIL STAGE";
    public static final String ROLLBACK_STAGE = "XA ROLLBACK STAGE";
    public static final String ROLLBACK_FAIL_STAGE = "XA ROLLBACK FAIL STAGE";

    protected final NonBlockingSession session;
    protected AbstractXAHandler xaHandler;

    XAStage(NonBlockingSession session, AbstractXAHandler handler) {
        this.session = session;
        this.xaHandler = handler;
    }

    public abstract void onEnterStage(MySQLConnection conn);

    @Override
    public void onEnterStage() {
        xaHandler.setUnResponseRrns();
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            onEnterStage((MySQLConnection) session.getTarget(rrn));
        }
    }

    protected void feedback(boolean isSuccess) {
        session.clearResources(false);
        if (session.closed()) {
            return;
        }

        if (isSuccess) {
            session.setFinishedCommitTime();
            ImplicitCommitHandler implicitCommitHandler = xaHandler.getImplicitCommitHandler();
            if (implicitCommitHandler != null) {
                xaHandler.clearResources();
                implicitCommitHandler.next();
                return;
            }
        }
        session.setResponseTime(isSuccess);
        byte[] sendData = xaHandler.getPacketIfSuccess();
        if (sendData != null) {
            session.getService().write(sendData);
        } else {
            session.getService().write(session.getOkByteArray());
        }
        xaHandler.clearResources();
    }

    // return ok
    public abstract void onConnectionOk(MySQLConnection conn);

    // connect error
    public abstract void onConnectionError(MySQLConnection conn, int errNo);

    // connect close
    public abstract void onConnectionClose(MySQLConnection conn);

    // connect error
    public abstract void onConnectError(MySQLConnection conn);

    public abstract String getStage();
}
