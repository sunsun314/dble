/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple;

import com.actiontech.dble.common.cache.LayerCachePool;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.service.server.ServerConnection;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

/**
 * RouteStrategy
 *
 * @author wang.dw
 */
public interface RouteStrategy {
    SQLStatement parserSQL(String originSql) throws SQLSyntaxErrorException;

    RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ServerConnection sc, LayerCachePool cachePool)
            throws SQLException;

    RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ServerConnection sc, LayerCachePool cachePool, boolean isExplain)
            throws SQLException;
}
