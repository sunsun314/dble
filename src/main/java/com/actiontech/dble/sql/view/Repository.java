/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.view;

import java.util.Map;

/**
 * Created by szf on 2017/10/12.
 */
public interface Repository {
    String SCHEMA_VIEW_SPLIT = ":";
    String SERVER_ID = "serverId";
    String CREATE_SQL = "createSql";
    String DELETE = "delete";
    String UPDATE = "update";
    String LOCK = "lock";

    Map<String, Map<String, String>> getViewCreateSqlMap();

    void put(String schemaName, String viewName, String createSql);

    void delete(String schemaName, String viewName);

    void init();

    void terminate();
}