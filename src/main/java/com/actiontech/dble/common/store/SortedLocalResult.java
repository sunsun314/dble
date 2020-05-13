/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.common.store;

import com.actiontech.dble.sql.handler.util.RowDataComparator;
import com.actiontech.dble.common.store.diskbuffer.SortedResultDiskBuffer;
import com.actiontech.dble.common.store.result.ResultExternal;
import com.actiontech.dble.common.buffer.BufferPool;

import java.util.Collections;

public class SortedLocalResult extends LocalResult {

    protected RowDataComparator rowCmp;

    public SortedLocalResult(BufferPool pool, int fieldsCount, RowDataComparator rowCmp, String charset) {
        this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, rowCmp, charset);
    }

    public SortedLocalResult(int initialCapacity, int fieldsCount, BufferPool pool, RowDataComparator rowCmp,
                             String charset) {
        super(initialCapacity, fieldsCount, pool, charset);
        this.rowCmp = rowCmp;
    }

    @Override
    protected ResultExternal makeExternal() {
        return new SortedResultDiskBuffer(pool, fieldsCount, rowCmp);
    }

    @Override
    protected void beforeFlushRows() {
        Collections.sort(rows, this.rowCmp);
    }

    @Override
    protected void doneOnlyMemory() {
        Collections.sort(rows, this.rowCmp);
    }

}
