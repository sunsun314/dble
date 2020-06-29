package newservices.manager.dump.handler;

import com.alibaba.druid.sql.ast.SQLStatement;
import newservices.manager.dump.DumpException;
import newservices.manager.dump.DumpFileContext;

import java.sql.SQLNonTransientException;

public interface StatementHandler {

    SQLStatement preHandle(DumpFileContext context, String stmt) throws DumpException, InterruptedException, SQLNonTransientException;

    void handle(DumpFileContext context, SQLStatement statement) throws DumpException, InterruptedException;

    void handle(DumpFileContext context, String stmt) throws DumpException, InterruptedException;
}
