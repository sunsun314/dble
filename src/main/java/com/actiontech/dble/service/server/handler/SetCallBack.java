/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.handler;

import com.actiontech.dble.service.ServerConnection;
import com.actiontech.dble.common.sqljob.SQLQueryResult;
import com.actiontech.dble.common.sqljob.SQLQueryResultListener;

import java.util.Map;

public class SetCallBack implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private ServerConnection sc;
    private boolean backToOtherThread;

    SetCallBack(ServerConnection sc) {
        this.sc = sc;
    }

    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (result.isSuccess()) {
            sc.executeContextSetTask();
            backToOtherThread = sc.executeInnerSetTask();
        } else {
            sc.getContextTask().clear();
            sc.getInnerSetTask().clear();
        }
    }

    public boolean isBackToOtherThread() {
        return backToOtherThread;
    }
}
