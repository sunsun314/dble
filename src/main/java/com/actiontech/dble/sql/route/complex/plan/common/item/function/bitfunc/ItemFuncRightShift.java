/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.complex.plan.common.item.function.bitfunc;

import com.actiontech.dble.sql.route.complex.plan.common.field.Field;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.sql.route.complex.plan.common.item.function.primary.ItemFuncBit;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncRightShift extends ItemFuncBit {

    public ItemFuncRightShift(Item a, Item b) {
        super(a, b);
    }

    @Override
    public final String funcName() {
        return ">>";
    }

    @Override
    public BigInteger valInt() {
        if (args.get(0).isNullValue()) {
            nullValue = true; /* purecov: inspected */
            return BigInteger.ZERO; /* purecov: inspected */
        }
        int shift = args.get(1).valInt().intValue();
        if (args.get(1).isNullValue()) {
            nullValue = true;
            return BigInteger.ZERO;
        }
        nullValue = false;

        BigInteger arg1 = args.get(0).valInt();
        return shift < Long.SIZE * 8 ? arg1.shiftRight(shift) : BigInteger.ZERO;
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLBinaryOpExpr(args.get(0).toExpression(), SQLBinaryOperator.RightShift, args.get(1).toExpression());
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncRightShift(newArgs.get(0), newArgs.get(1));
    }

}