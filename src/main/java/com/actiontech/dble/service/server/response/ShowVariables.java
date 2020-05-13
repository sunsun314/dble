/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.server.response;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.sql.handler.ShowVariablesHandler;
import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.sql.route.simple.util.RouterUtil;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.server.parser.ServerParse;
import com.actiontech.dble.service.util.SchemaUtil;

public final class ShowVariables {
    private ShowVariables() {
    }

    public static void response(ServerConnection c, String stmt) {
        String db = c.getSchema() != null ? c.getSchema() : SchemaUtil.getRandomDb();

        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            c.writeErrMessage("42000", "Unknown database '" + db + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        RouteResultset rrs = new RouteResultset(stmt, ServerParse.SHOW);
        try {
            RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode());
            ShowVariablesHandler handler = new ShowVariablesHandler(rrs, c.getSession2());
            try {
                handler.execute();
            } catch (Exception e1) {
                handler.recycleBuffer();
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e1.toString());
            }
        } catch (Exception e) {
            // Could this only be ER_PARSE_ERROR?
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.toString());
        }
    }
}
