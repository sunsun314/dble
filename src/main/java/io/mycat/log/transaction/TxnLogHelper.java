package io.mycat.log.transaction;

import io.mycat.MycatServer;
import io.mycat.server.ServerConnection;

public class TxnLogHelper {
	public static void putTxnLog(ServerConnection c, String sql){
		if (MycatServer.getInstance().getConfig().getSystem().isRecordTxn()) {
			MycatServer.getInstance().getTxnLogProcessor().putTxnLog(c, sql);
		}
	}
}
