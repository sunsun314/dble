/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.assistant.backend;

import com.actiontech.dble.common.net.ClosableConnection;
import com.actiontech.dble.sql.handler.ResponseHandler;
import com.actiontech.dble.sql.route.simple.RouteResultsetNode;
import com.actiontech.dble.service.server.NonBlockingSession;
import com.actiontech.dble.service.server.ServerConnection;

import java.io.UnsupportedEncodingException;

public interface BackendConnection extends ClosableConnection {
    boolean isDDL();

    boolean isFromSlaveDB();

    String getSchema();

    void setSchema(String newSchema);

    long getLastTime();

    void setAttachment(Object attachment);

    void setLastTime(long currentTimeMillis);

    void release();

    boolean setResponseHandler(ResponseHandler commandHandler);

    void setSession(NonBlockingSession session);

    void commit();

    void query(String sql) throws UnsupportedEncodingException;

    void query(String query, boolean isAutocommit);

    Object getAttachment();

    // long getThreadId();


    void execute(RouteResultsetNode node, ServerConnection source,
                 boolean autocommit);

    boolean syncAndExecute();

    void rollback();

    boolean isBorrowed();

    void setBorrowed(boolean borrowed);

    int getTxIsolation();

    boolean isAutocommit();

    long getId();

    void closeWithoutRsp(String reason);

    String compactInfo();

    void setOldTimestamp(long oldTimestamp);

    void setExecuting(boolean executing);

    boolean isExecuting();

    void disableRead();

    void enableRead();
}
