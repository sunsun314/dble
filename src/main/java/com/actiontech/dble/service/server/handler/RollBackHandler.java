/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.server.handler;

import com.actiontech.dble.assistant.transactionlog.TxnLogHelper;
import com.actiontech.dble.service.server.ServerConnection;

public final class RollBackHandler {
    private RollBackHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        TxnLogHelper.putTxnLog(c, stmt);
        c.getSession2().transactionsCount();
        c.rollback();
    }
}
