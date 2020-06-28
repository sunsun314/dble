/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.server.status.SlowQueryLog;
import newcommon.proto.mysql.packet.OkPacket;
import newservices.manager.ManagerService;
import newservices.manager.handler.WriteDynamicBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class OnOffSlowQueryLog {
    private OnOffSlowQueryLog() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffSlowQueryLog.class);

    public static void execute(ManagerService service, boolean isOn) {
        String onOffStatus = isOn ? "enable" : "disable";
        try {
            WriteDynamicBootstrap.getInstance().changeValue("enableSlowLog", isOn ? "1" : "0");
        } catch (IOException e) {
            String msg = onOffStatus + " slow_query_log failed";
            LOGGER.warn(String.valueOf(service) + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        SlowQueryLog.getInstance().setEnableSlowLog(isOn);
        LOGGER.info(String.valueOf(service) + " " + onOffStatus + " slow_query_log success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage((onOffStatus + " slow_query_log success").getBytes());
        ok.write(service);
    }

}
