/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.handler.util;

import com.actiontech.dble.assistant.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;

public class HeapItem {
    private byte[] row;
    private RowDataPacket rowPacket;
    private MySQLConnection hashIndex;
    private boolean isNull = false;

    public static HeapItem nullItem() {
        HeapItem nullItem = new HeapItem(null, null, null);
        nullItem.isNull = true;
        return nullItem;
    }

    public boolean isNullItem() {
        return row == null && isNull;
    }

    public HeapItem(byte[] row, RowDataPacket rdp, MySQLConnection index) {
        this.row = row;
        this.rowPacket = rdp;
        this.hashIndex = index;
    }

    public MySQLConnection getIndex() {
        return hashIndex;
    }

    public byte[] getRowData() {
        return row;
    }

    public RowDataPacket getRowPacket() {
        return this.rowPacket;
    }

}
