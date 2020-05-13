/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.handler;

import com.actiontech.dble.assistant.backend.BackendConnection;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.service.server.NonBlockingSession;
import com.actiontech.dble.common.util.StringUtil;

import java.util.Map;

public class ShowVariablesHandler extends SingleNodeHandler {
    private Map<String, String> shadowVars;

    public ShowVariablesHandler(RouteResultset rrs, NonBlockingSession session) {
        super(rrs, session);
        shadowVars = session.getSource().getSysVariables();
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        String charset = session.getSource().getCharset().getResults();
        RowDataPacket rowDataPacket = new RowDataPacket(2);
        /* Fixme: the accurate statistics of netOutBytes.
         *
         * We read net packet, but don't do Stat here. So the statistical magnitude -- netOutBytes is not exact.
         * Of course, we can do that.
         * But it is tiresome: re-implement the function of method rowResponse that have been implemented in super class.
         */
        rowDataPacket.read(row);
        String varName = StringUtil.decode(rowDataPacket.fieldValues.get(0), charset);
        if (shadowVars.containsKey(varName)) {
            rowDataPacket.setValue(1, StringUtil.encode(shadowVars.get(varName), charset));
            super.rowResponse(rowDataPacket.toBytes(), rowPacket, isLeft, conn);
        } else {
            super.rowResponse(row, rowPacket, isLeft, conn);
        }
        return false;
    }
}
