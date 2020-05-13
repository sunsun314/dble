/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.handler;

import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.sql.route.simple.route.factory.RouteStrategyFactory;
import com.actiontech.dble.sql.route.simple.route.util.RouterUtil;
import com.actiontech.dble.service.ServerConnection;
import com.actiontech.dble.sql.parser.ServerParse;
import com.actiontech.dble.service.util.SchemaUtil;
import com.actiontech.dble.service.util.SchemaUtil.SchemaInfo;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;

public final class DescribeHandler {
    private DescribeHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        try {
            SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            MySqlExplainStatement describeStatement = (MySqlExplainStatement) statement;
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(c.getUser(), c.getSchema(), describeStatement.getTableName(), null);
            c.routeSystemInfoAndExecuteSQL(RouterUtil.removeSchema(stmt, schemaInfo.getSchema()), schemaInfo, ServerParse.DESCRIBE);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
            return;
        }
    }
}
