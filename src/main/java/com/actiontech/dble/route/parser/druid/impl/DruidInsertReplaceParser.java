/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.RouteTableConfigInfo;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.ArrayUtil;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.ConditionUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.handler.ExplainHandler;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.mpp.ColumnRoute;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.stat.TableStat;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

import static com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;

abstract class DruidInsertReplaceParser extends DefaultDruidParser {

    abstract SQLSelect acceptVisitor(SQLStatement stmt, ServerSchemaStatVisitor visitor);

    abstract int tryGetShardingColIndex(SchemaInfo schemaInfo, SQLStatement stmt, String partitionColumn) throws SQLNonTransientException;


    private void tryRouteInsertQuery(ServerConnection sc, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, SchemaInfo schemaInfo, SchemaConfig schema) throws SQLException {
        // insert into .... select ....
        SQLSelect select = acceptVisitor(stmt, visitor);
        String tableName = schemaInfo.getTable();
        TableConfig tc = schema.getTables().get(tableName);
        String schemaName = schema == null ? null : schema.getName();

        if (tc == null || tc.isNoSharding() || (tc.isGlobalTable() && tc.getDataNodes().size() == 1)) {
            //only require when all the table and the route condition route to same node
            Map<String, String> tableAliasMap = getTableAliasMap(schemaName, visitor.getAliasMap());
            ctx.setRouteCalculateUnits(ConditionUtil.buildRouteCalculateUnits(visitor.getAllWhereUnit(), tableAliasMap, schemaName));
            checkForSingleNodeTable(visitor, tc, rrs);
            RouterUtil.routeToSingleNode(rrs, tc.getDataNodes().get(0));
        } else if (tc.isGlobalTable() && tc.getDataNodes().size() > 1) {
            checkForMultiNodeGlobal(visitor, tc, rrs, schema);
        } else {
            checkForShardingTable(visitor, select, sc, rrs, tc, schemaInfo, stmt, schema);
        }
    }


    private void routeForSourceTable(TableConfig tc, RouteResultset rrs, Set<String> allNodeSet, SchemaConfig schema,
                                     RouteCalculateUnit routeUnit, ArrayList<String> partNodeList) throws SQLException {
        RouteResultset rrsTmp = RouterUtil.tryRouteForOneTable(schema, routeUnit, tc.getName(), rrs, true);
        if (rrsTmp != null && rrsTmp.getNodes() != null) {
            for (RouteResultsetNode n : rrsTmp.getNodes()) {
                partNodeList.add(n.getName());
                allNodeSet.add(n.getName());
            }
        }
    }

    private boolean checkForERRelationship() {
        return false;
    }

    private void checkForShardingSingleRouteUnit(Map<String, ArrayList<String>> notShardingTableMap, RouteCalculateUnit routeUnit, RouteTableConfigInfo dataSourceTc,
                                                 ServerSchemaStatVisitor visitor, Map<String, String> tableAliasMap, Set<String> allNodeSet, SchemaConfig schema,
                                                 TableConfig tc, RouteResultset rrs) throws SQLException {
        //不同的OR条件分割，可能存在对于单个OR条件中的列出的relation无法正确识别的情况，认了
        Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null) {
            //对于每一个table的条件
            //直接按照我们的数据来源主表组成条件
            Pair<String, String> key = new Pair<>(dataSourceTc.getSchema(), dataSourceTc.getTableConfig().getName());
            if (dataSourceTc.getTableConfig() != null && dataSourceTc.getTableConfig().getRule() != null) {
                if (!ArrayUtil.containDuplicate(visitor.getSelectTableList(), dataSourceTc.getTableConfig().getName())) {
                    ArrayList<String> partNodeList = new ArrayList<String>();
                    routeForSourceTable(tc, rrs, allNodeSet, schema, routeUnit, partNodeList);
                    //直接对于剩下的所有表进行判断
                    //不能直接循环条件，而是要从关键的table list入手
                    for (Pair<String, String> tn : ctx.getTables()) {
                        if (tn.equals(key)) {
                            //如果是上文的source表,跳过这个表的检查
                            continue;
                        } else if (ArrayUtil.containDuplicate(visitor.getSelectTableList(), dataSourceTc.getTableConfig().getName())) {
                            String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                            throw new SQLNonTransientException(msg);
                        }

                        String sName = tn.getKey();
                        String tName = tn.getValue();
                        SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
                        TableConfig tConfig = tSchema.getTables().get(tName);
                        //如果是分片表
                        if (tConfig != null && tConfig.getRule() != null) {
                            if (dataSourceTc.getTableConfig().getRule().equals(tConfig.getRule())) {
                                //规则相同则要求和来源表以分片键关联
                                //具体怎么做还是要看debug出来的情况
                                boolean hasReleation = false;
                                TableStat.Column sourceColumn = new TableStat.Column(dataSourceTc.getAlias() == null ? dataSourceTc.getTableConfig().getName() : dataSourceTc.getAlias()
                                        , dataSourceTc.getTableConfig().getRule().getColumn());
                                ArrayList<String> rsAlias = findAliasByMap(tableAliasMap, tName);
                                List<TableStat.Column> rsColumnList = new ArrayList<>();
                                for (String rsAlia : rsAlias) {
                                    rsColumnList.add(new TableStat.Column(rsAlia, tConfig.getRule().getColumn()));
                                }
                                for (TableStat.Relationship rs : visitor.getRelationships()) {
                                    if (rs.getLeft().equals(sourceColumn) && rs.getOperator().equals("=")) {
                                        for (TableStat.Column rsColumn : rsColumnList) {
                                            if (rs.getRight().equals(rsColumn)) {
                                                hasReleation = true;
                                                break;
                                            }
                                        }

                                    } else if (rs.getRight().equals(sourceColumn) && rs.getOperator().equals("=")) {
                                        for (TableStat.Column rsColumn : rsColumnList) {
                                            if (rs.getLeft().equals(rsColumn)) {
                                                hasReleation = true;
                                                break;
                                            }
                                        }
                                    }
                                    if (hasReleation) {
                                        break;
                                    }
                                }
                                if (!hasReleation) {
                                    String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                                    throw new SQLNonTransientException(msg);
                                }
                            } else {
                                //规则不统一直接报错，不商量
                                String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                                throw new SQLNonTransientException(msg);
                            }
                        } else if (tConfig != null && tConfig.getParentKey() != null && tConfig.getRule() == null) {
                            //如果是ER子表,只需要它的JOINKEY关联回去即可，因为后续会有其他的检查的

                        } else if (tConfig != null && tConfig.isGlobalTable() && tConfig.getDataNodes().size() > 1) {
                            //如果是多节点global表
                            notShardingTableMap.put(tConfig.getName(), tConfig.getDataNodes());
                            if (!ArrayUtil.containAll(tConfig.getDataNodes(), partNodeList)) {
                                String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                                throw new SQLNonTransientException(msg);
                            }
                        } else if (tConfig == null || tConfig.getDataNodes().size() == 1 && tConfig.getRule() == null) {
                            //如果是单节点的全局表或者是默认节点的表
                            notShardingTableMap.put(tConfig.getName(), tConfig.getDataNodes());
                            if (!ArrayUtil.containAll(tConfig.getDataNodes(), partNodeList)) {
                                String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                                throw new SQLNonTransientException(msg);
                            }
                        }
                    }
                } else {
                    String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                    throw new SQLNonTransientException(msg);
                }
            }
        }
    }

    private void checkShardingKeyConstant(RouteTableConfigInfo dataSourceTc, RouteResultset rrs, ServerSchemaStatVisitor visitor,
                                          String tableName, TableConfig tc, SchemaConfig schema) throws SQLException {
        //这个里面判断如果是一个固定值的话,直接路由信息
        RouteCalculateUnit singleRouteUnit = new RouteCalculateUnit();
        singleRouteUnit.addShardingExpr(new Pair<String, String>(dataSourceTc.getSchema(), tableName), tc.getRule().getColumn(), dataSourceTc.getValue());
        Set<String> allNodeSet = new HashSet<>();
        RouteResultset rrsTmp = RouterUtil.tryRouteForOneTable(schema, singleRouteUnit, tc.getName(), rrs, true);
        if (rrsTmp != null && rrsTmp.getNodes() != null) {
            for (RouteResultsetNode n : rrsTmp.getNodes()) {
                allNodeSet.add(n.getName());
            }
        }
        if (allNodeSet.size() > 1) {
            String msg = "This `INSERT ... SELECT Syntax` is not supported!";
            throw new SQLNonTransientException(msg);
        }
        checkForSingleNodeTable(visitor, tc, rrs);
        RouterUtil.routeToSingleNode(rrs, rrsTmp.getNodes()[0].getName());
        return;

    }

    private void checkForShardingTable(ServerSchemaStatVisitor visitor, SQLSelect select, ServerConnection sc, RouteResultset rrs,
                                       TableConfig tc, SchemaInfo schemaInfo, SQLStatement stmt, SchemaConfig schema) throws SQLException {
        //the insert table is a sharding table
        String tableName = schemaInfo.getTable();
        String schemaName = schema == null ? null : schema.getName();

        MySQLPlanNodeVisitor pvisitor = new MySQLPlanNodeVisitor(sc.getSchema(), sc.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), false, sc.getUsrVariables());
        pvisitor.visit(select);
        PlanNode node = pvisitor.getTableNode();
        node.setSql(rrs.getStatement());
        node.setUpFields();

        //获取到拆分列的index，从sql或者是其他
        String partitionColumn = tc.getPartitionColumn();
        int index = tryGetShardingColIndex(schemaInfo, stmt, partitionColumn);
        try {
            RouteTableConfigInfo dataSourceTc = node.findFieldSourceFromIndex(index);
            //判断两边的列是不是有ER关系
            if (dataSourceTc.getTableConfig() == null) {
                checkShardingKeyConstant(dataSourceTc, rrs, visitor, tableName, tc, schema);
            } else if (dataSourceTc.getTableConfig().getRule() != null && dataSourceTc.getTableConfig().getRule().equals(tc.getRule())) {
                //然后先计算这个TABLE_s的涉及路由节点
                Map<String, String> tableAliasMap = getTableAliasMap(schemaName, visitor.getAliasMap());
                ctx.setRouteCalculateUnits(ConditionUtil.buildRouteCalculateUnits(visitor.getAllWhereUnit(), tableAliasMap, schemaName));

                //全局的条件，所有的都需要满足这个内容
                Map<String, ArrayList<String>> notShardingTableMap = new HashMap<>();
                Set<String> allNodeSet = new HashSet<>();

                for (RouteCalculateUnit routeUnit : ctx.getRouteCalculateUnits()) {
                    checkForShardingSingleRouteUnit(notShardingTableMap, routeUnit, dataSourceTc, visitor, tableAliasMap, allNodeSet, schema, tc, rrs);
                }

                for (Map.Entry<String, ArrayList<String>> entry : notShardingTableMap.entrySet()) {
                    if (!CollectionUtil.containAll(entry.getValue(), allNodeSet)) {
                        String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                        throw new SQLNonTransientException(msg);
                    }
                }

                RouterUtil.routeToMultiNode(false, rrs, allNodeSet, true);
                return;

            } else {
                throw new Exception("not support");
            }

        } catch (Exception e) {
            String msg = "This `INSERT ... SELECT Syntax` is not supported!";
            throw new SQLNonTransientException(msg);
        }
    }

    private void checkForMultiNodeGlobal(ServerSchemaStatVisitor visitor, TableConfig tc, RouteResultset rrs, SchemaConfig schema) throws SQLNonTransientException {
        //multi-Node global table
        ArrayList mustContainList = tc.getDataNodes();
        for (String sTable : visitor.getSelectTableList()) {
            TableConfig stc = schema.getTables().get(sTable);
            if (stc.isGlobalTable()) {
                if (stc != null && ArrayUtil.containAll(stc.getDataNodes(), mustContainList)) {
                    continue;
                } else {
                    String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                    throw new SQLNonTransientException(msg);
                }
            }
        }
        //route to all the dataNode the tc contain
        RouterUtil.routeToMultiNode(false, rrs, mustContainList, true);
    }

    private void checkForSingleNodeTable(ServerSchemaStatVisitor visitor, TableConfig tc, RouteResultset rrs) throws SQLNonTransientException {
        Set<Pair<String, String>> tablesSet = new HashSet<>(ctx.getTables());

        //loop for the tables & conditions
        for (RouteCalculateUnit routeUnit : ctx.getRouteCalculateUnits()) {
            Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
            if (tablesAndConditions != null) {
                for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
                    Pair<String, String> table = entry.getKey();
                    String sName = table.getKey();
                    String tName = table.getValue();
                    SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
                    TableConfig tConfig = tSchema.getTables().get(tName);
                    if (tConfig != null && tConfig.getRule() != null) {
                        if (!ArrayUtil.containDuplicate(visitor.getSelectTableList(), tName)) {
                            Set<String> tmpResultNodes = new HashSet<>();
                            tmpResultNodes.add(tc.getDataNodes().get(0));
                            if (!RouterUtil.tryCalcNodeForShardingColumn(rrs, tmpResultNodes, tablesSet, entry, table, tConfig)) {
                                String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                                throw new SQLNonTransientException(msg);
                            }
                        } else {
                            String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                            throw new SQLNonTransientException(msg);
                        }
                    }

                }
            }
        }

        for (Pair<String, String> table : tablesSet) {
            String sName = table.getKey();
            String tName = table.getValue();
            SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
            TableConfig tConfig = tSchema.getTables().get(tName);
            if (tConfig == null) {
                if (tSchema.getDataNode().equals(tc.getDataNodes().get(0))) {
                    break;
                } else {
                    String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                    throw new SQLNonTransientException(msg);
                }
            } else if (tConfig.isGlobalTable()) {
                if (ArrayUtil.contains(tConfig.getDataNodes().toArray(new String[]{}), tc.getDataNodes().get(0))) {
                    break;
                } else {
                    String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                    throw new SQLNonTransientException(msg);
                }
            } else {
                String msg = "This `INSERT ... SELECT Syntax` is not supported!";
                throw new SQLNonTransientException(msg);
            }
        }
    }


    static RouteResultset routeByERParentKey(RouteResultset rrs, TableConfig tc, String joinKeyVal, SchemaInfo schemaInfo)
            throws SQLNonTransientException {
        if (tc.getDirectRouteTC() != null) {
            ColumnRoute columnRoute = new ColumnRoute(joinKeyVal);
            checkDefaultValues(joinKeyVal, tc, schemaInfo.getSchema(), tc.getJoinKey());
            Set<String> dataNodeSet = RouterUtil.ruleCalculate(rrs, tc.getDirectRouteTC(), columnRoute, false);
            if (dataNodeSet.size() != 1) {
                throw new SQLNonTransientException("parent key can't find  valid data node ,expect 1 but found: " + dataNodeSet.size());
            }
            String dn = dataNodeSet.iterator().next();
            if (SQLJob.LOGGER.isDebugEnabled()) {
                SQLJob.LOGGER.debug("found partion node (using parent partition rule directly) for child table to insert  " + dn + " sql :" + rrs.getStatement());
            }
            return RouterUtil.routeToSingleNode(rrs, dn);
        }
        return null;
    }


    /**
     * check if the column is not null and the
     */
    static void checkDefaultValues(String columnValue, TableConfig tableConfig, String schema, String partitionColumn) throws SQLNonTransientException {
        if (columnValue == null || "null".equalsIgnoreCase(columnValue)) {
            TableMeta meta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schema, tableConfig.getName());
            for (TableMeta.ColumnMeta columnMeta : meta.getColumns()) {
                if (!columnMeta.isCanNull()) {
                    if (columnMeta.getName().equalsIgnoreCase(partitionColumn)) {
                        String msg = "Sharding column can't be null when the table in MySQL column is not null";
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg);
                    }
                }
            }
        }
    }

    static String shardingValueToSting(SQLExpr valueExpr) throws SQLNonTransientException {
        String shardingValue = null;
        if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr intExpr = (SQLIntegerExpr) valueExpr;
            shardingValue = intExpr.getNumber() + "";
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = (SQLCharExpr) valueExpr;
            shardingValue = charExpr.getText();
        }

        if (shardingValue == null && !(valueExpr instanceof SQLNullExpr)) {
            throw new SQLNonTransientException("Not Supported of Sharding Value EXPR :" + valueExpr.toString());
        }
        return shardingValue;
    }


    int getIncrementKeyIndex(SchemaInfo schemaInfo, String incrementColumn) throws SQLNonTransientException {
        if (incrementColumn == null) {
            throw new SQLNonTransientException("please make sure the incrementColumn's config is not null in schemal.xml");
        }
        TableMeta tbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                schemaInfo.getTable());
        if (tbMeta != null) {
            for (int i = 0; i < tbMeta.getColumns().size(); i++) {
                if (incrementColumn.equalsIgnoreCase(tbMeta.getColumns().get(i).getName())) {
                    return i;
                }
            }
            String msg = "please make sure your table structure has incrementColumn";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        return -1;
    }

    int getTableColumns(SchemaInfo schemaInfo, List<SQLExpr> columnExprList)
            throws SQLNonTransientException {
        if (columnExprList == null || columnExprList.size() == 0) {
            TableMeta tbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
            if (tbMeta == null) {
                String msg = "Meta data of table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            return tbMeta.getColumns().size();
        } else {
            return columnExprList.size();
        }
    }

    int getShardingColIndex(SchemaInfo schemaInfo, List<SQLExpr> columnExprList, String partitionColumn) throws SQLNonTransientException {
        int shardingColIndex = -1;
        if (columnExprList == null || columnExprList.size() == 0) {
            TableMeta tbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
            if (tbMeta != null) {
                for (int i = 0; i < tbMeta.getColumns().size(); i++) {
                    if (partitionColumn.equalsIgnoreCase(tbMeta.getColumns().get(i).getName())) {
                        return i;
                    }
                }
            }
            return shardingColIndex;
        }
        for (int i = 0; i < columnExprList.size(); i++) {
            if (partitionColumn.equalsIgnoreCase(StringUtil.removeBackQuote(columnExprList.get(i).toString()))) {
                return i;
            }
        }
        return shardingColIndex;
    }

    protected void logAndThrowException(String message) throws SQLNonTransientException {

    }

    protected static ArrayList<String> findAliasByMap(Map<String, String> tableAliasMap, String name) {
        ArrayList<String> x = new ArrayList<>();
        for (Map.Entry<String, String> entry : tableAliasMap.entrySet()) {
            if (entry.getValue().equalsIgnoreCase(name)) {
                x.add(entry.getKey());
            }
        }
        return x;
    }

    void fetchChildTableToRoute(TableConfig tc, String joinKeyVal, ServerConnection sc, SchemaConfig schema, String sql, RouteResultset rrs, boolean isExplain) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            //get child result will be blocked, so use ComplexQueryExecutor
            @Override
            public void run() {
                // route by sql query root parent's data node
                String findRootTBSql = tc.getLocateRTableKeySql() + joinKeyVal;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("to find root parent's node sql :" + findRootTBSql);
                }
                FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler(findRootTBSql, sc.getSession2());
                try {
                    String dn = fetchHandler.execute(schema.getName(), tc.getRootParent().getDataNodes());
                    if (dn == null) {
                        sc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "can't find (root) parent sharding node for sql:" + sql);
                        return;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("found partition node for child table to insert " + dn + " sql :" + sql);
                    }
                    RouterUtil.routeToSingleNode(rrs, dn);
                    if (isExplain) {
                        ExplainHandler.writeOutHeadAndEof(sc, rrs);
                    } else {
                        sc.getSession2().execute(rrs);
                    }
                } catch (ConnectionException e) {
                    sc.setTxInterrupt(e.toString());
                    sc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.toString());
                }
            }
        });
    }
}
