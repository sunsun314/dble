/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.server.handler;

import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.server.parser.ServerParse;
import com.actiontech.dble.service.server.parser.ServerParseStart;

/**
 * @author mycat
 */
public final class StartHandler {
    private StartHandler() {
    }

    public static void handle(String stmt, ServerConnection c, int offset) {
        switch (ServerParseStart.parse(stmt, offset)) {
            case ServerParseStart.TRANSACTION:
                BeginHandler.handle(stmt, c);
                break;
            case ServerParseStart.READCHARCS:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                break;
            default:
                c.execute(stmt, ServerParse.START);
        }
    }

}
