package com.actiontech.dble.service.response;

import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.service.ServerConnection;

import java.util.List;

/**
 * Created by szf on 2019/5/30.
 */
public interface InnerFuncResponse {

    List<FieldPacket> getField();

    List<RowDataPacket> getRows(ServerConnection c);

}
