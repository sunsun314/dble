/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.response;

import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.sql.route.simple.route.factory.RouteStrategyFactory;
import com.actiontech.dble.sql.route.simple.route.util.RouterUtil;
import com.actiontech.dble.service.ServerConnection;
import com.actiontech.dble.sql.parser.ServerParse;
import com.actiontech.dble.service.util.SchemaUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement;

/**
 * Created by huqing.yan on 2017/7/19.
 */
public final class ShowCreateTable {
    private ShowCreateTable() {
    }

    public static void response(ServerConnection c, String stmt) {
        try {
            SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            MySqlShowCreateTableStatement showCreateTableStatement = (MySqlShowCreateTableStatement) statement;
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(c.getUser(), c.getSchema(), showCreateTableStatement.getName(), null);
            c.routeSystemInfoAndExecuteSQL(RouterUtil.removeSchema(stmt, schemaInfo.getSchema()), schemaInfo, ServerParse.SHOW);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
