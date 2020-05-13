/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.common.mysql.variables;

import com.actiontech.dble.common.sqljob.SQLQueryResult;
import com.actiontech.dble.common.sqljob.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MysqlVarsListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlVarsListener.class);
    private final VarsExtractorHandler handler;

    public MysqlVarsListener(VarsExtractorHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (!result.isSuccess()) {
            //not thread safe
            LOGGER.warn("Can't get variables from DataNode: " + result.getDataNode() + "!");
            handler.signalDone();
            return;
        }

        Map<String, String> kvs = result.getResult();
        handler.handleVars(kvs);
    }
}
