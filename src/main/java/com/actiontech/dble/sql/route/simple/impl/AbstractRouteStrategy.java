/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.route.impl;

import com.actiontech.dble.common.cache.LayerCachePool;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.route.RouteResultset;
import com.actiontech.dble.sql.route.simple.route.RouteStrategy;
import com.actiontech.dble.sql.route.simple.route.util.RouterUtil;
import com.actiontech.dble.service.ServerConnection;
import com.actiontech.dble.sql.parser.ServerParse;
import com.actiontech.dble.common.bean.LoadData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public abstract class AbstractRouteStrategy implements RouteStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String origSQL,
                                ServerConnection sc, LayerCachePool cachePool, boolean isExplain) throws SQLException {

        RouteResultset rrs = new RouteResultset(origSQL, sqlType);


        /*
         * debug mode and load data ,no cache
         */
        if (LOGGER.isDebugEnabled() && origSQL.startsWith(LoadData.LOAD_DATA_HINT)) {
            rrs.setSqlRouteCacheAble(false);
        }

        if (sqlType == ServerParse.CALL) {
            rrs.setCallStatement(true);
        }

        if (schema == null) {
            rrs = routeNormalSqlWithAST(null, origSQL, rrs, cachePool, sc, isExplain);
        } else {
            if (sqlType == ServerParse.SHOW) {
                rrs.setStatement(origSQL);
                rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode());
            } else {
                rrs = routeNormalSqlWithAST(schema, origSQL, rrs, cachePool, sc, isExplain);
            }
        }

        return rrs;
    }


    /**
     * routeNormalSqlWithAST
     */
    public abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs,
                                                         LayerCachePool cachePool, ServerConnection sc, boolean isExplain) throws SQLException;


}
