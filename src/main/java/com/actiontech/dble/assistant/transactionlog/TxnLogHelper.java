/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.assistant.transactionlog;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.service.server.ServerConnection;

public final class TxnLogHelper {
    private TxnLogHelper() {
    }

    public static void putTxnLog(ServerConnection c, String sql) {
        if (DbleServer.getInstance().getConfig().getSystem().getRecordTxn() == 1) {
            DbleServer.getInstance().getTxnLogProcessor().putTxnLog(c, sql);
        }
    }
}
