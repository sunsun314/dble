/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.handler;

import com.actiontech.dble.common.net.FrontendConnection;
import com.actiontech.dble.common.net.NIOHandler;

/**
 * ManagerAuthenticator
 *
 * @author mycat
 */
public class ManagerAuthenticator extends FrontendAuthenticator {
    public ManagerAuthenticator(FrontendConnection source) {
        super(source);
    }

    @Override
    protected NIOHandler successCommendHandler() {
        return new ManagerCommandHandler(source);
    }
}
