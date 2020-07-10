/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.mysqlsharding.backend.xa.recovery;

import newservices.mysqlsharding.backend.xa.CoordinatorLogEntry;

import java.util.Collection;

/**
 * Created by zhangchao on 2016/10/13.
 */
public interface Repository {

    void init();

    void put(String id, CoordinatorLogEntry coordinatorLogEntry);

    void remove(String id);

    CoordinatorLogEntry get(String coordinatorId);

    Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries(boolean first);

    boolean writeCheckpoint(Collection<CoordinatorLogEntry> checkpointContent);

    void close();

}
