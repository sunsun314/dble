package com.actiontech.dble.sql.handler.transaction;

public interface TransactionStage {

    void onEnterStage();

    TransactionStage next(boolean isFail, String errMsg, byte[] errPacket);

}
