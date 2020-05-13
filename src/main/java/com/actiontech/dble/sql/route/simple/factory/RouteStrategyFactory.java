/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.factory;

import com.actiontech.dble.sql.route.simple.RouteStrategy;
import com.actiontech.dble.sql.route.simple.impl.DefaultRouteStrategy;

/**
 * RouteStrategyFactory
 *
 * @author wang.dw
 */
public final class RouteStrategyFactory {
    private static RouteStrategy defaultStrategy = new DefaultRouteStrategy();

    private RouteStrategyFactory() {

    }


    public static RouteStrategy getRouteStrategy() {
        return defaultStrategy;
    }
}
