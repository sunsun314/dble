/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.handler;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.assistant.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.common.cache.LayerCachePool;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.sql.route.simple.util.RouterUtil;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * HintDataNodeHandler
 *
 * @author zhuam
 */
public class HintDataNodeHandler implements HintHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintDataNodeHandler.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ServerConnection sc,
                                LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLNonTransientException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("route datanode sql hint from " + realSQL);
        }

        RouteResultset rrs = new RouteResultset(realSQL, sqlType);
        if (ServerParse.CALL == sqlType) {
            rrs.setCallStatement(true);
        }
        PhysicalDataNode dataNode = DbleServer.getInstance().getConfig().getDataNodes().get(hintSQLValue);
        if (dataNode != null) {
            rrs = RouterUtil.routeToSingleNode(rrs, dataNode.getName());
        } else {
            String msg = "can't find hint datanode:" + hintSQLValue;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        return rrs;
    }

}
