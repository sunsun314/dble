/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.handler;

import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.common.mysql.packet.OkPacket;
import com.actiontech.dble.sql.route.simple.route.factory.RouteStrategyFactory;
import com.actiontech.dble.service.ServerConnection;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;

/**
 * Created by collapsar on 2019/07/23.
 */
public final class FlushTableHandler {

    private FlushTableHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        try {
            stmt = stmt.replace("/*!", "/*#");
            MySqlFlushStatement statement = (MySqlFlushStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.write(c);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }
}
