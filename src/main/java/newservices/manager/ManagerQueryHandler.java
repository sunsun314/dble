/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package newservices.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public class ManagerQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerQueryHandler.class);
    private static final int SHIFT = 8;
    private final ManagerService service;

    private Boolean readOnly = true;

    public ManagerQueryHandler(ManagerService source) {
        this.service = source;
    }


    public void query(String sql) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.valueOf(service) + sql);
        }
        /*
        int rs = ManagerParse.parse(sql);
        int sqlType = rs & 0xff;
        if (readOnly && sqlType != ManagerParse.SELECT && sqlType != ManagerParse.SHOW) {
            service.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User READ ONLY");
            return;
        }
        switch (sqlType) {
            case ManagerParse.SELECT:
                SelectHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.SET:
                //todo set 什么都应该返回OK
                //service.write(service.writeToBuffer(OkPacket.OK, service.allocate()));
                break;
            case ManagerParse.SHOW:
                ShowHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.KILL_CONN:
                KillConnection.response(sql, rs >>> SHIFT, service);
                break;
            case ManagerParse.KILL_XA_SESSION:
                KillXASession.response(sql, rs >>> SHIFT, service);
                break;
            case ManagerParse.KILL_DDL_LOCK:
                String tableInfo = sql.substring(rs >>> SHIFT).trim();
                KillDdlLock.response(sql, tableInfo, service);
                break;
            case ManagerParse.OFFLINE:
                Offline.execute(service);
                break;
            case ManagerParse.ONLINE:
                Online.execute(service);
                break;
            case ManagerParse.PAUSE:
                PauseStart.execute(service, sql);
                break;
            case ManagerParse.RESUME:
                PauseEnd.execute(service);
                break;
            case ManagerParse.STOP:
                StopHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.DRY_RUN:
                DryRun.execute(service);
                break;
            case ManagerParse.RELOAD:
                ReloadHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.ROLLBACK:
                RollbackHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.CONFIGFILE:
                ConfFileHandler.handle(sql, service);
                break;
            case ManagerParse.LOGFILE:
                ShowServerLog.handle(sql, service);
                break;
            case ManagerParse.CREATE_DB:
                DatabaseHandler.handle(sql, service, true);
                break;
            case ManagerParse.DROP_DB:
                DatabaseHandler.handle(sql, service, false);
                break;
            case ManagerParse.ENABLE:
                EnableHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.DISABLE:
                DisableHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.CHECK:
                CheckHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.RELEASE_RELOAD_METADATA:
                ReleaseReloadMetadata.execute(service);
                break;
            case ManagerParse.DB_GROUP:
                DbGroupHAHandler.handle(sql, service);
                break;
            case ManagerParse.SPLIT:
                // todo 空闲检测需要处理 service.skipIdleCheck(true);
                SplitDumpHandler.handle(sql, service, rs >>> SHIFT);
                break;
            case ManagerParse.FLOW_CONTROL:
                FlowControlHandler.handle(sql, service);
                break;
            default:
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }*/
    }

}