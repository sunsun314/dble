/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.parser.druid.impl.ddl;

import com.actiontech.dble.common.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.parser.druid.impl.DefaultDruidParser;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.sql.route.simple.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.sql.route.simple.util.RouterUtil;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.util.SchemaUtil;
import com.actiontech.dble.service.util.SchemaUtil.SchemaInfo;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;

import java.sql.SQLException;

public class DruidTruncateTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain)
            throws SQLException {
        rrs.setDdlType(DDLInfo.DDLType.TRUNCATE_TABLE);
        String schemaName = schema == null ? null : schema.getName();
        SQLTruncateStatement truncateTable = (SQLTruncateStatement) stmt;
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, truncateTable.getTableSources().get(0));
        String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        rrs.setStatement(statement);
        String noShardingNode = RouterUtil.isNoShardingDDL(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
        if (noShardingNode != null) {
            RouterUtil.routeToSingleDDLNode(schemaInfo, rrs, noShardingNode);
            return schemaInfo.getSchemaConfig();
        }
        RouterUtil.routeToDDLNode(schemaInfo, rrs);
        return schemaInfo.getSchemaConfig();
    }
}