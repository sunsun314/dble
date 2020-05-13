package com.actiontech.dble.assistant.backend.mysql.nio.handler.query.impl.join;

import com.actiontech.dble.sql.route.complex.plan.Order;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.service.NonBlockingSession;

import java.util.List;

/**
 * Created by szf on 2019/5/31.
 */
public class JoinInnerHandler extends JoinHandler {

    public JoinInnerHandler(long id, NonBlockingSession session, boolean isLeftJoin, List<Order> leftOrder, List<Order> rightOrder, Item otherJoinOn) {
        super(id, session, isLeftJoin, leftOrder, rightOrder, otherJoinOn);
    }
}
