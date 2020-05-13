/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.sql.route.complex.plan.common.item.function.timefunc;

import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.sql.route.complex.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.sql.route.complex.plan.common.item.function.ItemFunc;
import com.actiontech.dble.sql.route.complex.plan.common.time.MySQLTime;

import java.util.List;

/**
 * timezone change,not support
 */
public class ItemFuncConvTz extends ItemDatetimeFunc {

    public ItemFuncConvTz(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "convert_tz";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unsupported function convert_tz!");
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncConvTz(realArgs);
    }

}
