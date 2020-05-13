package com.actiontech.dble.sql.route.complex.plan.common.item.function;

import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.sql.route.complex.plan.common.item.Item;
import com.actiontech.dble.sql.route.complex.plan.common.time.MySQLTime;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.service.server.response.InnerFuncResponse;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Created by szf on 2019/5/28.
 */
public class ItemFuncInner extends ItemFunc {

    private final InnerFuncResponse rspHandler;
    private final String funcName;

    public ItemFuncInner(String funcName, List<Item> args, InnerFuncResponse rspHandler) {
        super(args);
        this.funcName = funcName;
        this.rspHandler = rspHandler;
    }

    public List<FieldPacket> getField() {
        return rspHandler.getField();
    }

    public List<RowDataPacket> getRows(ServerConnection connection) {
        return rspHandler.getRows(connection);
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    public String funcName() {
        return funcName;
    }

    @Override
    public BigDecimal valReal() {
        return null;
    }

    @Override
    public BigInteger valInt() {
        return null;
    }

    @Override
    public String valStr() {
        return null;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return false;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return false;
    }
}
