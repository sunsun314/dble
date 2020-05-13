/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.sql.route.complex.plan.common.item.function.mathsfunc;

import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.sql.route.complex.plan.common.item.function.ItemFunc;
import com.actiontech.dble.sql.route.complex.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;

public class ItemFuncExp extends ItemDecFunc {

    /**
     * @param args
     */
    public ItemFuncExp(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "exp";
    }

    @Override
    public BigDecimal valReal() {
        BigDecimal value = args.get(0).valReal();
        if (nullValue = args.get(0).isNullValue()) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(Math.exp(value.doubleValue()));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncExp(realArgs);
    }
}