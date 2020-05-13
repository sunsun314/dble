package com.actiontech.dble.management.dump.handler;

import com.actiontech.dble.management.dump.DumpException;
import com.actiontech.dble.management.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLNonTransientException;

public interface StatementHandler {

    SQLStatement preHandle(DumpFileContext context, String stmt) throws DumpException, InterruptedException, SQLNonTransientException;

    void handle(DumpFileContext context, SQLStatement statement) throws DumpException, InterruptedException;

    void handle(DumpFileContext context, String stmt) throws DumpException, InterruptedException;
}
