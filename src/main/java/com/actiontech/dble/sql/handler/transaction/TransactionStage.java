package com.actiontech.dble.assistant.backend.mysql.nio.handler.transaction;

public interface TransactionStage {

    void onEnterStage();

    TransactionStage next(boolean isFail, String errMsg, byte[] errPacket);

}
