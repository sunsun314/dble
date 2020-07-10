/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.mysqlsharding.backend.store;

import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.RBTreeList;
import newservices.mysqlsharding.backend.nio.handler.util.RowDataComparator;

public class DistinctSortedLocalResult extends DistinctLocalResult {
    public DistinctSortedLocalResult(BufferPool pool, int fieldsCount, RowDataComparator distinctCmp, String charset) {
        super(pool, fieldsCount, distinctCmp, charset);
    }

    /**
     * @return next row
     */
    @Override
    public RowDataPacket next() {
        lock.lock();
        try {
            if (this.isClosed)
                return null;
            if (++rowId < rowCount) {
                if (external != null) {
                    currentRow = external.next();
                } else {
                    currentRow = ((RBTreeList<RowDataPacket>) rows).inOrderOf(rowId);
                }
            } else {
                currentRow = null;
            }
            return currentRow;
        } finally {
            lock.unlock();
        }
    }
}
