/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.management.response;

import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.service.manager.ManagerConnection;
import com.actiontech.dble.common.mysql.packet.OkPacket;
import com.actiontech.dble.assistant.slowlog.SlowQueryLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReloadSlowQueryTime {
    private ReloadSlowQueryTime() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadSlowQueryTime.class);

    public static void execute(ManagerConnection c, int time) {
        if (time < 0) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the commend is not correct");
            return;
        }

        SlowQueryLog.getInstance().setSlowTime(time);
        LOGGER.info(String.valueOf(c) + " reload @@slow_query.time=" + time + " success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("reload @@slow_query.time success".getBytes());
        ok.write(c);
    }

}
