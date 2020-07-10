/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.mysqlsharding.backend.store;

import com.actiontech.dble.buffer.BufferPool;
import newservices.mysqlsharding.backend.store.diskbuffer.UnSortedResultDiskBuffer;
import newservices.mysqlsharding.backend.store.result.ResultExternal;

public class UnSortedLocalResult extends LocalResult {

    public UnSortedLocalResult(int fieldsCount, BufferPool pool, String charset) {
        this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, charset);
    }

    public UnSortedLocalResult(int initialCapacity, int fieldsCount, BufferPool pool, String charset) {
        super(initialCapacity, fieldsCount, pool, charset);
    }

    @Override
    protected ResultExternal makeExternal() {
        return new UnSortedResultDiskBuffer(pool, fieldsCount);
    }

    @Override
    protected void beforeFlushRows() {

    }

    @Override
    protected void doneOnlyMemory() {

    }

}
