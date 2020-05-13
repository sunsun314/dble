/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.parser.druid;

import com.actiontech.dble.common.cache.LayerCachePool;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.service.server.ServerConnection;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * Parser SQLStatement
 *
 * @author wang.dw
 */
public interface DruidParser {
    /**
     * use MycatSchemaStatVisitor, get the info of tables,tableAliasMap,conditions and so on
     *
     * @param schema
     * @param stmt
     * @param sc
     */
    SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql, LayerCachePool cachePool, ServerSchemaStatVisitor schemaStatVisitor, ServerConnection sc) throws SQLException;


    /**
     * use MycatSchemaStatVisitor, get the info of tables,tableAliasMap,conditions and so on
     *
     * @param schema
     * @param stmt
     * @param sc
     */
    SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql, LayerCachePool cachePool, ServerSchemaStatVisitor schemaStatVisitor, ServerConnection sc, boolean isExplain) throws SQLException;

    /**
     * @param stmt
     * @param sc
     */
    SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain) throws SQLException;

    /**
     * changeSql: add limit
     *
     * @param schema
     * @param rrs
     * @param stmt
     * @throws SQLNonTransientException
     */
    void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, LayerCachePool cachePool) throws SQLException;

    /**
     * get parser info
     *
     * @return
     */
    DruidShardingParseInfo getCtx();

}
