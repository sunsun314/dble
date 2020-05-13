/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.common.net;

public interface SocketAcceptor {

    void start();

    String getName();

    int getPort();

}