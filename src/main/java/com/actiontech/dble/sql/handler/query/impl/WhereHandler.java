/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.handler.query.impl;

import com.actiontech.dble.assistant.backend.BackendConnection;
import com.actiontech.dble.sql.handler.query.BaseDMLHandler;
import com.actiontech.dble.sql.handler.util.HandlerTool;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.sql.route.complex.plan.common.field.Field;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.service.server.NonBlockingSession;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class WhereHandler extends BaseDMLHandler {

    public WhereHandler(long id, NonBlockingSession session, Item where) {
        super(id, session);
        assert (where != null);
        this.where = where;
    }

    private Item where = null;
    private List<Field> sourceFields;
    // if merge handler have no order by, the row response is not thread safe
    private ReentrantLock lock = new ReentrantLock();

    @Override
    public HandlerType type() {
        return HandlerType.WHERE;
    }

    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, BackendConnection conn) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        this.fieldPackets = fieldPackets;
        this.sourceFields = HandlerTool.createFields(this.fieldPackets);
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, conn);
    }

    public boolean rowResponse(byte[] rowNull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        lock.lock();
        try {
            HandlerTool.initFields(this.sourceFields, rowPacket.fieldValues);
            Item whereItem = HandlerTool.createItem(this.where, this.sourceFields, 0, this.isAllPushDown(), this.type());
            /* use whereto filter */
            if (whereItem.valBool()) {
                nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
            } else {
                // nothing
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(data, isLeft, conn);
    }

    @Override
    public void onTerminate() {
    }


}
