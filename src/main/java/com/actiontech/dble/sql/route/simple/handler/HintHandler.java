/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.handler;

import com.actiontech.dble.common.cache.LayerCachePool;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.service.server.ServerConnection;

import java.sql.SQLException;
import java.util.Map;

/**
 * router according to  the hint
 */
public interface HintHandler {

    RouteResultset route(SchemaConfig schema,
                         int sqlType, String realSQL, ServerConnection sc,
                         LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException;
}
