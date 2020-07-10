package newservices.mysqlsharding.backend.nio.handler.transaction;

public interface TransactionStage {

    void onEnterStage();

    TransactionStage next(boolean isFail, String errMsg, byte[] errPacket);

}
