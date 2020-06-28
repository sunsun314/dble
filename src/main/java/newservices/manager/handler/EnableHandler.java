/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.parser.ManagerParseOnOff;
import newservices.manager.ManagerService;
import newservices.manager.response.OnOffAlert;
import newservices.manager.response.OnOffCustomMySQLHa;
import newservices.manager.response.OnOffSlowQueryLog;

public final class EnableHandler {
    private EnableHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        int rs = ManagerParseOnOff.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseOnOff.SLOW_QUERY_LOG:
                OnOffSlowQueryLog.execute(service, true);
                break;
            case ManagerParseOnOff.ALERT:
                OnOffAlert.execute(service, true);
                break;
            case ManagerParseOnOff.CUSTOM_MYSQL_HA:
                OnOffCustomMySQLHa.execute(service, true);
                break;
            default:
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }

    }
}
