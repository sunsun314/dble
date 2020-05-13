/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.assistant.backend.mysql.nio.handler;

import com.actiontech.dble.assistant.backend.BackendConnection;

/**
 * Created by nange on 2015/3/31.
 */
public interface LoadDataResponseHandler {
    void requestDataResponse(byte[] row, BackendConnection conn);
}
