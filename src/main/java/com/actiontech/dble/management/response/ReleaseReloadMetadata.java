package com.actiontech.dble.management.response;

import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.service.manager.ManagerConnection;
import com.actiontech.dble.sql.meta.ReloadManager;
import com.actiontech.dble.common.mysql.packet.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2019/7/16.
 */
public final class ReleaseReloadMetadata {
    private ReleaseReloadMetadata() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReleaseReloadMetadata.class);

    public static void execute(ManagerConnection c) {
        LOGGER.info("Command reload@@release received");
        //check status only if the server is in reloading & reload in RELOAD_STATUS_META_RELOAD
        if (ReloadManager.checkCanRelease()) {
            //try to interrupt the dble reload
            if (!ReloadManager.interruptReload()) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "Reloading finished or other client interrupt the reload");
                return;
            }
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "Dble not in reloading or reload status not interruptible");
            return;
        }

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage(("reload release success,please reload @@metadata to make meta update").getBytes());
        ok.write(c);
    }

}
