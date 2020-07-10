/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.mysqlsharding.backend.nio.handler.transaction;

public interface TransactionHandler {
    void commit();

    void implicitCommit(ImplicitCommitHandler handler);

    void rollback();

    void turnOnAutoCommit(byte[] previousSendData);
}
