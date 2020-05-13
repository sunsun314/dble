/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.common.store;

import com.actiontech.dble.common.store.diskbuffer.UnSortedResultDiskBuffer;
import com.actiontech.dble.common.store.result.ResultExternal;
import com.actiontech.dble.common.buffer.BufferPool;

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