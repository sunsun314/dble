/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.parser.druid.impl.ddl;

import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.sql.route.simple.parser.druid.impl.DefaultDruidParser;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.sql.route.simple.util.RouterUtil;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.util.SchemaUtil;
import com.actiontech.dble.service.util.SchemaUtil.SchemaInfo;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class DruidCreateIndexParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
                                     ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain) throws SQLException {
        SQLCreateIndexStatement createStmt = (SQLCreateIndexStatement) stmt;
        SQLTableSource tableSource = createStmt.getTable();
        if (tableSource instanceof SQLExprTableSource) {
            String schemaName = schema == null ? null : schema.getName();
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, (SQLExprTableSource) tableSource);
            String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
            rrs.setStatement(statement);
            String noShardingNode = RouterUtil.isNoShardingDDL(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
            if (noShardingNode != null) {
                RouterUtil.routeToSingleDDLNode(schemaInfo, rrs, noShardingNode);
                return schemaInfo.getSchemaConfig();
            }
            RouterUtil.routeToDDLNode(schemaInfo, rrs);
            return schemaInfo.getSchemaConfig();
        } else {
            String msg = "The DDL is not supported, sql:" + stmt;
            throw new SQLNonTransientException(msg);
        }
    }
}
