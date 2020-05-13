package com.actiontech.dble.sql.handler.query.impl;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.assistant.backend.BackendConnection;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.service.server.NonBlockingSession;

import java.util.List;

/**
 * Created by szf on 2019/5/29.
 */
public class MultiNodeFakeHandler extends MultiNodeMergeHandler {

    private FakeBaseSelectHandler execHandler;

    public MultiNodeFakeHandler(long id, NonBlockingSession session, List<Item> selectList, boolean partOfUnion) {
        super(id, session);
        this.execHandler = new FakeBaseSelectHandler(id, session, selectList, this, partOfUnion);
        this.merges.add(this);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        session.setHandlerStart(this);
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, conn);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        return nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        if (this.terminate.get())
            return;
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(null, this.isLeft, conn);
    }

    @Override
    public void execute() throws Exception {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                execHandler.fakeExecute();
            }
        });
    }

    @Override
    protected void ownThreadJob(Object... objects) {

    }

    @Override
    protected void terminateThread() throws Exception {

    }

    @Override
    protected void recycleResources() {

    }

    public String toSQLString() {
        return execHandler.toSQLString();
    }


    @Override
    public HandlerType type() {
        return HandlerType.FAKE_MERGE;
    }
}
