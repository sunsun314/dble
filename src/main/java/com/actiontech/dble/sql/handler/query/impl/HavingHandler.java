/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.handler.query.impl;

import com.actiontech.dble.assistant.backend.BackendConnection;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.sql.handler.query.BaseDMLHandler;
import com.actiontech.dble.sql.handler.query.DMLResponseHandler;
import com.actiontech.dble.sql.handler.util.HandlerTool;
import com.actiontech.dble.sql.route.complex.plan.common.field.Field;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.service.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * having is just as same as where
 *
 * @author ActionTech
 */
public class HavingHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(HavingHandler.class);

    public HavingHandler(long id, NonBlockingSession session, Item having) {
        super(id, session);
        assert (having != null);
        this.having = having;
    }

    private Item having = null;
    private Item havingItem = null;
    private List<Field> sourceFields;
    private ReentrantLock lock = new ReentrantLock();

    @Override
    public DMLResponseHandler.HandlerType type() {
        return DMLResponseHandler.HandlerType.HAVING;
    }

    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, BackendConnection conn) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        this.fieldPackets = fieldPackets;
        this.sourceFields = HandlerTool.createFields(this.fieldPackets);
        /**
         * having will not be pushed down because of aggregate function
         */
        this.havingItem = HandlerTool.createItem(this.having, this.sourceFields, 0, false, this.type());
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, conn);
    }

    public boolean rowResponse(byte[] rowNull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        lock.lock();
        try {
            HandlerTool.initFields(this.sourceFields, rowPacket.fieldValues);
            /* filter by having statement */
            if (havingItem.valBool()) {
                nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
            } else {
                // nothing
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        LOGGER.debug("roweof");
        if (terminate.get())
            return;
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(data, isLeft, conn);
    }

    @Override
    public void onTerminate() {
    }
}
