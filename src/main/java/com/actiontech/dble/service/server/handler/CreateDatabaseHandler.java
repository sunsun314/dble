/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.server.handler;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.common.mysql.packet.OkPacket;
import com.actiontech.dble.sql.route.simple.factory.RouteStrategyFactory;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.common.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;

/**
 * Created by collapsar on 2019/07/23.
 */
public final class CreateDatabaseHandler {

    private CreateDatabaseHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        try {
            stmt = stmt.replace("/*!", "/*#");
            SQLCreateDatabaseStatement statement = (SQLCreateDatabaseStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            String schema = statement.getName().getSimpleName();
            schema = StringUtil.removeBackQuote(schema);
            SchemaConfig sc = DbleServer.getInstance().getConfig().getSchemas().get(schema);
            if (sc != null) {
                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.write(c);
            } else {
                throw new Exception("Can't create database '" + schema + "' that doesn't exists in schema.xml");
            }
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }
}
