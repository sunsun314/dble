/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.parser.druid.impl.ddl;

import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.common.config.model.TableConfig;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.sql.route.simple.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.sql.route.simple.parser.druid.impl.DefaultDruidParser;
import com.actiontech.dble.sql.route.simple.util.RouterUtil;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.util.SchemaUtil;
import com.actiontech.dble.service.util.SchemaUtil.SchemaInfo;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DruidAlterTableParser
 *
 * @author wang.dw
 */
public class DruidAlterTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain)
            throws SQLException {
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, alterTable.getTableSource());
        boolean support = false;
        String msg = "The DDL is not supported, sql:";
        for (SQLAlterTableItem alterItem : alterTable.getItems()) {
            if (alterItem instanceof SQLAlterTableAddColumn ||
                    alterItem instanceof SQLAlterTableAddIndex ||
                    alterItem instanceof SQLAlterTableDropIndex ||
                    alterItem instanceof SQLAlterTableDropKey ||
                    alterItem instanceof SQLAlterTableDropPrimaryKey) {
                support = true;
            } else if (alterItem instanceof SQLAlterTableAddConstraint) {
                SQLConstraint constraint = ((SQLAlterTableAddConstraint) alterItem).getConstraint();
                if (constraint instanceof MySqlPrimaryKey) {
                    support = true;
                }
            } else if (alterItem instanceof MySqlAlterTableChangeColumn ||
                    alterItem instanceof MySqlAlterTableModifyColumn ||
                    alterItem instanceof SQLAlterTableDropColumnItem) {
                List<SQLName> columnList = new ArrayList<>();
                if (alterItem instanceof MySqlAlterTableChangeColumn) {
                    columnList.add(((MySqlAlterTableChangeColumn) alterItem).getColumnName());
                } else if (alterItem instanceof MySqlAlterTableModifyColumn) {
                    columnList.add(((MySqlAlterTableModifyColumn) alterItem).getNewColumnDefinition().getName());
                } else if (alterItem instanceof SQLAlterTableDropColumnItem) {
                    columnList = ((SQLAlterTableDropColumnItem) alterItem).getColumns();
                }
                support = !this.columnInfluenceCheck(columnList, schemaInfo.getSchemaConfig(), schemaInfo.getTable());
                if (!support) {
                    msg = "The columns may be sharding keys or ER keys, are not allowed to alter sql:";
                }
            }
        }
        if (!support) {
            msg = msg + stmt;
            throw new SQLNonTransientException(msg);
        }
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


    /**
     * the function is check if the columns contains the import column
     * true -- yes the sql did not to exec
     * false -- safe the sql can be exec
     */
    private boolean columnInfluenceCheck(List<SQLName> columnList, SchemaConfig schema, String table) {
        for (SQLName name : columnList) {
            if (this.influenceKeyColumn(name, schema, table)) {
                return true;
            }
        }
        return false;
    }

    /**
     * this function is check if the name is the important column in any tables
     * true -- the column influence some important column
     * false -- safe
     */
    private boolean influenceKeyColumn(SQLName name, SchemaConfig schema, String tableName) {
        String columnName = name.toString();
        Map<String, TableConfig> tableConfig = schema.getTables();
        TableConfig changedTable = tableConfig.get(tableName);
        if (changedTable == null) {
            return false;
        }
        if (columnName.equalsIgnoreCase(changedTable.getPartitionColumn()) ||
                columnName.equalsIgnoreCase(changedTable.getJoinKey())) {
            return true;
        }
        // Traversal all the table node to find if some table is the child table of the changedTale
        for (Map.Entry<String, TableConfig> entry : tableConfig.entrySet()) {
            TableConfig tb = entry.getValue();
            if (tb.getParentTC() != null &&
                    tableName.equalsIgnoreCase(tb.getParentTC().getName()) &&
                    columnName.equalsIgnoreCase(tb.getParentKey())) {
                return true;
            }
        }
        return false;
    }

}