/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.handler;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.assistant.backend.BackendConnection;
import com.actiontech.dble.sql.handler.query.DMLResponseHandler;
import com.actiontech.dble.sql.handler.util.HandlerTool;
import com.actiontech.dble.management.handler.PackageBufINf;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.sql.route.complex.plan.common.field.Field;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.sql.route.complex.plan.visitor.MySQLItemVisitor;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.service.server.NonBlockingSession;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.server.response.ShowTables;
import com.actiontech.dble.service.server.response.ShowTablesStmtInfo;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.common.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by huqing.yan on 2017/7/20.
 */
public class ShowTablesHandler extends SingleNodeHandler {
    private String showTableSchema;
    private Map<String, String> shardingTablesMap;
    private Item whereItem;
    private List<Field> sourceFields;
    private ShowTablesStmtInfo info;
    public ShowTablesHandler(RouteResultset rrs, NonBlockingSession session, ShowTablesStmtInfo info) {
        super(rrs, session);
        buffer = session.getSource().allocate();
        this.info = info;
        ServerConnection source = session.getSource();
        String showSchema = info.getSchema();
        if (showSchema != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            showSchema = showSchema.toLowerCase();
        }
        showTableSchema = showSchema == null ? source.getSchema() : showSchema;
        shardingTablesMap = ShowTables.getTableSet(showTableSchema, info);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        ServerConnection source = session.getSource();
        PackageBufINf bufInf;
        lock.lock();
        try {
            if (writeToClient.get()) {
                return;
            }
            if (info.isFull()) {
                List<FieldPacket> fieldPackets = new ArrayList<>(2);
                bufInf = ShowTables.writeFullTablesHeader(buffer, source, showTableSchema, fieldPackets);
                packetId = bufInf.getPacketId();
                buffer = bufInf.getBuffer();
                if (info.getWhere() != null) {
                    MySQLItemVisitor mev = new MySQLItemVisitor(source.getSchema(), source.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), source.getUsrVariables());
                    info.getWhereExpr().accept(mev);
                    sourceFields = HandlerTool.createFields(fieldPackets);
                    whereItem = HandlerTool.createItem(mev.getItem(), sourceFields, 0, false, DMLResponseHandler.HandlerType.WHERE);
                    bufInf = ShowTables.writeFullTablesRow(buffer, source, shardingTablesMap, packetId, whereItem, sourceFields);
                    packetId = bufInf.getPacketId();
                    buffer = bufInf.getBuffer();
                } else {
                    bufInf = ShowTables.writeFullTablesRow(buffer, source, shardingTablesMap, packetId, null, null);
                    packetId = bufInf.getPacketId();
                    buffer = bufInf.getBuffer();
                }
            } else {
                bufInf = ShowTables.writeTablesHeaderAndRows(buffer, source, shardingTablesMap, showTableSchema);
                packetId = bufInf.getPacketId();
                buffer = bufInf.getBuffer();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        RowDataPacket rowDataPacket = new RowDataPacket(1);
        rowDataPacket.read(row);
        String table = StringUtil.decode(rowDataPacket.fieldValues.get(0), session.getSource().getCharset().getResults());
        if (shardingTablesMap.containsKey(table)) {
            this.netOutBytes += row.length;
            this.selectRows++;
        } else {
            if (whereItem != null) {
                RowDataPacket rowDataPk = new RowDataPacket(sourceFields.size());
                rowDataPk.read(row);
                HandlerTool.initFields(sourceFields, rowDataPk.fieldValues);
                /* filter the where condition */
                if (whereItem.valBool()) {
                    super.rowResponse(row, rowPacket, isLeft, conn);
                }
            } else {
                super.rowResponse(row, rowPacket, isLeft, conn);
            }
        }
        return false;
    }
}