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
import com.actiontech.dble.sql.route.complex.plan.common.item.FieldTypes;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.service.server.NonBlockingSession;
import com.actiontech.dble.common.util.StringUtil;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * if the Item is Item_sum,then theItem must be generated in GroupBy.Otherwise,calc by middle-ware
 */
public class SendMakeHandler extends BaseDMLHandler {

    private final ReentrantLock lock;

    private List<Item> selects;
    private List<Field> sourceFields;
    private List<Item> selItems;
    private String tableAlias;
    private String table;
    private String schema;

    public SendMakeHandler(long id, NonBlockingSession session, List<Item> selects, String schema, String table, String tableAlias) {
        super(id, session);
        lock = new ReentrantLock();
        this.selects = selects;
        this.selItems = new ArrayList<>();
        this.schema = schema;
        this.table = table;
        this.tableAlias = tableAlias;
    }

    @Override
    public HandlerType type() {
        return HandlerType.SENDMAKER;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            session.setHandlerStart(this);
            if (terminate.get())
                return;
            this.fieldPackets = fieldPackets;
            this.sourceFields = HandlerTool.createFields(this.fieldPackets);
            for (Item sel : selects) {
                Item tmpItem = HandlerTool.createItem(sel, this.sourceFields, 0, isAllPushDown(), type());
                tmpItem.setItemName(sel.getItemName());
                String selAlias = sel.getAlias();
                if (selAlias != null) {
                    // remove the added tmp FNAF
                    selAlias = StringUtil.removeApostropheOrBackQuote(selAlias);
                    if (StringUtils.indexOf(selAlias, Item.FNAF) == 0)
                        selAlias = StringUtils.substring(selAlias, Item.FNAF.length());
                }
                tmpItem = HandlerTool.createRefItem(tmpItem, schema, table, tableAlias, selAlias);
                this.selItems.add(tmpItem);
            }
            List<FieldPacket> newFieldPackets = new ArrayList<>();
            for (Item selItem : this.selItems) {
                FieldPacket tmpFp = new FieldPacket();
                selItem.makeField(tmpFp);
                /* Keep things compatible for old clients */
                if (tmpFp.getType() == FieldTypes.MYSQL_TYPE_VARCHAR.numberValue())
                    tmpFp.setType(FieldTypes.MYSQL_TYPE_VAR_STRING.numberValue());
                newFieldPackets.add(tmpFp);
            }
            nextHandler.fieldEofResponse(null, null, newFieldPackets, null, this.isLeft, conn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get())
                return true;
            HandlerTool.initFields(sourceFields, rowPacket.fieldValues);
            RowDataPacket newRp = new RowDataPacket(selItems.size());
            for (Item selItem : selItems) {
                byte[] b = selItem.getRowPacketByte();
                newRp.add(b);
            }
            nextHandler.rowResponse(null, newRp, this.isLeft, conn);
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get())
                return;
            session.setHandlerEnd(this);
            nextHandler.rowEofResponse(eof, this.isLeft, conn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onTerminate() {
    }


}