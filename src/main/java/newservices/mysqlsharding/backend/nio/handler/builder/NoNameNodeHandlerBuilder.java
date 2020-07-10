/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package newservices.mysqlsharding.backend.nio.handler.builder;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFuncInner;
import com.actiontech.dble.plan.node.MergeNode;
import com.actiontech.dble.plan.node.NoNameNode;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import newservices.mysqlsharding.backend.nio.handler.builder.sqlvisitor.PushDownVisitor;
import newservices.mysqlsharding.backend.nio.handler.query.DMLResponseHandler;
import newservices.mysqlsharding.backend.nio.handler.query.impl.MultiNodeEasyMergeHandler;
import newservices.mysqlsharding.backend.nio.handler.query.impl.MultiNodeFakeHandler;
import newservices.mysqlsharding.backend.nio.handler.query.impl.MultiNodeMergeHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * query like "select 1 as name"
 *
 * @author ActionTech
 * @CreateTime 2015/3/23
 */
class NoNameNodeHandlerBuilder extends BaseHandlerBuilder {
    private NoNameNode node;

    protected NoNameNodeHandlerBuilder(NonBlockingSession session, NoNameNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
        this.needWhereHandler = false;
        this.needCommon = false;
    }

    @Override
    protected void handleSubQueries() {
        handleBlockingSubQuery();
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        return new ArrayList<>();
    }

    @Override
    public void buildOwn() {
        PushDownVisitor visitor = new PushDownVisitor(node, true);
        visitor.visit();
        this.canPushDown = true;
        String sql = visitor.getSql().toString();
        String schema = session.getService().getSchema();
        SchemaConfig schemaConfig = schemaConfigMap.get(schema);
        String randomDatenode = getRandomNode(schemaConfig.getAllShardingNodes());
        RouteResultsetNode[] rrss = new RouteResultsetNode[]{new RouteResultsetNode(randomDatenode, ServerParse.SELECT, sql)};
        hBuilder.checkRRSs(rrss);
        MultiNodeMergeHandler mh = new MultiNodeEasyMergeHandler(getSequenceId(), rrss, session.getService().isAutocommit() && !session.getService().isTxStart(), session);
        addHandler(mh);
    }

    @Override
    protected void noShardBuild() {
        this.needCommon = false;
        //if the node is NoNameNode
        boolean allSelectInnerFunc = true;
        for (Item i : this.node.getColumnsSelected()) {
            if (!(i instanceof ItemFuncInner)) {
                allSelectInnerFunc = false;
                break;
            }
        }
        if (allSelectInnerFunc) {
            boolean union = false;
            if (this.node.getParent() instanceof MergeNode) {
                union = ((MergeNode) this.node.getParent()).isUnion();
            }
            MultiNodeMergeHandler mh = new MultiNodeFakeHandler(getSequenceId(), session, this.node.getColumnsSelected(), union);
            addHandler(mh);
            return;
        }
        super.noShardBuild();

    }

}
