/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.manager.handler;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.parser.ManagerParseSelect;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import newservices.manager.ManagerService;
import newservices.manager.TransformSQLJob;
import newservices.manager.response.*;

import java.util.Iterator;

import static com.actiontech.dble.route.parser.ManagerParseSelect.*;

public final class SelectHandler {
    private SelectHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        int rs = ManagerParseSelect.parse(stmt, offset);
        switch (rs & 0xff) {
            case VERSION_COMMENT:
               //todo when the sharding is finished change this SelectVersionComment.response(service);
                break;
            case SESSION_TX_READ_ONLY:
                SelectSessionTxReadOnly.execute(service, stmt.substring(offset).trim());
                break;
            case SESSION_TRANSACTION_READ_ONLY:
                SelectSessionTxReadOnly.execute(service, stmt.substring(offset).trim());
                break;
            case MAX_ALLOWED_PACKET:
                SelectMaxAllowedPacket.execute(service);
                break;
            case TIMEDIFF:
                ShowSingleString.execute(service, stmt.substring(rs >>> 8), "00:00:00");
                break;
            default:
                if (isSupportSelect(stmt)) {
                    Iterator<PhysicalDbGroup> iterator = DbleServer.getInstance().getConfig().getDbGroups().values().iterator();
                    if (iterator.hasNext()) {
                        PhysicalDbGroup pool = iterator.next();
                        final PhysicalDbInstance source = pool.getWriteSource();
                        TransformSQLJob sqlJob = new TransformSQLJob(stmt, null, source, service);
                        sqlJob.run();
                    } else {
                        service.writeErrMessage(ErrorCode.ER_YES, "no valid dbGroup/dbInstance");
                    }
                } else {
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                }
        }

    }

    private static boolean isSupportSelect(String stmt) {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        SQLStatement statement = parser.parseStatement();
        if (!(statement instanceof SQLSelectStatement)) {
            return false;
        }

        SQLSelectQuery sqlSelectQuery = ((SQLSelectStatement) statement).getSelect().getQuery();
        if (!(sqlSelectQuery instanceof MySqlSelectQueryBlock)) {
            return false;
        }
        MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) sqlSelectQuery;
        SQLTableSource mysqlFrom = selectQueryBlock.getFrom();
        if (mysqlFrom != null) {
            return false;
        }
        for (SQLSelectItem item : selectQueryBlock.getSelectList()) {
            SQLExpr selectItem = item.getExpr();
            if (!isVariantRef(selectItem)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isVariantRef(SQLExpr expr) {
        if (expr instanceof SQLVariantRefExpr) {
            return true;
        } else if (expr instanceof SQLPropertyExpr) {
            return isVariantRef(((SQLPropertyExpr) expr).getOwner());
        } else {
            return false;
        }
    }
}
