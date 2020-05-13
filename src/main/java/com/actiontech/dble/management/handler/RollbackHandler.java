/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.management.handler;

import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.service.manager.ManagerConnection;
import com.actiontech.dble.management.response.RollbackConfig;
import com.actiontech.dble.service.manager.parser.ManagerParseRollback;

/**
 * @author mycat
 */
public final class RollbackHandler {

    private RollbackHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseRollback.parse(stmt, offset)) {
            case ManagerParseRollback.CONFIG:
                RollbackConfig.execute(c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
