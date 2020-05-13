/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.complex.plan.common.item.function.timefunc;

import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.sql.route.complex.plan.common.item.function.ItemFunc;
import com.actiontech.dble.sql.route.complex.plan.common.item.function.primary.ItemIntFunc;
import com.actiontech.dble.sql.route.complex.plan.common.time.MySQLTime;
import com.actiontech.dble.sql.route.complex.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncDatediff extends ItemIntFunc {

    public ItemFuncDatediff(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "datediff";
    }

    @Override
    public BigInteger valInt() {
        MySQLTime ltime1 = new MySQLTime();
        MySQLTime ltime2 = new MySQLTime();
        if (args.get(0).isNullValue() || args.get(1).isNullValue() || args.get(0).getDate(ltime1, MyTime.TIME_FUZZY_DATE) ||
                args.get(1).getDate(ltime2, MyTime.TIME_FUZZY_DATE)) {
            nullValue = true;
            return BigInteger.ZERO;
        }
        java.util.Calendar cal1 = ltime1.toCalendar();
        java.util.Calendar cal2 = ltime2.toCalendar();
        long diff = (cal1.getTimeInMillis() - cal2.getTimeInMillis()) / (24 * 3600);
        return BigInteger.valueOf(diff);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncDatediff(realArgs);
    }

}