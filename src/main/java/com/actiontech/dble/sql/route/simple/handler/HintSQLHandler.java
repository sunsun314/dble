/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.handler;

import com.actiontech.dble.common.cache.LayerCachePool;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.sql.route.simple.RouteResultsetNode;
import com.actiontech.dble.sql.route.simple.RouteStrategy;
import com.actiontech.dble.sql.route.simple.factory.RouteStrategyFactory;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.server.parser.ServerParse;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Map;

/**
 * HintSQLHandler
 */
public class HintSQLHandler implements HintHandler {

    private RouteStrategy routeStrategy;

    public HintSQLHandler() {
        this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ServerConnection sc,
                                LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException {

        RouteResultset rrs = routeStrategy.route(schema, hintSqlType,
                hintSQLValue, sc, cachePool);

        if (rrs.isNeedOptimizer()) {
            throw new SQLSyntaxErrorException("Complex SQL not supported in hint");
        }
        // replace the sql of RRS
        if (ServerParse.CALL == sqlType) {
            rrs.setCallStatement(true);
        }

        RouteResultsetNode[] oldRsNodes = rrs.getNodes();
        RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[oldRsNodes.length];
        for (int i = 0; i < newRrsNodes.length; i++) {
            newRrsNodes[i] = new RouteResultsetNode(oldRsNodes[i].getName(), sqlType, realSQL);
        }
        rrs.setNodes(newRrsNodes);

        return rrs;
    }

}
