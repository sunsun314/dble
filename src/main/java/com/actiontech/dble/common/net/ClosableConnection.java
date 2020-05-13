/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.common.net;

import com.actiontech.dble.common.mysql.packet.CharsetNames;

public interface ClosableConnection {
    CharsetNames getCharset();

    /**
     * close connection
     */
    void close(String reason);

    boolean isClosed();

    void idleCheck();

    long getStartupTime();

    String getHost();

    int getPort();

    int getLocalPort();

    long getNetInBytes();

    long getNetOutBytes();
}
