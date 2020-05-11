/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.singleton.TraceManager;
import io.opentracing.Span;

import java.nio.ByteBuffer;

public class WriteToBackendTask {
    private final MySQLConnection conn;
    private final CommandPacket packet;

    public WriteToBackendTask(MySQLConnection conn, CommandPacket packet) {
        this.conn = conn;
        this.packet = packet;
    }

    public void execute() {
        Span span = null;
        if (conn.getHandlerSpan() != null) {
            span = TraceManager.getTracer().buildSpan("Write-To-Backend").asChildOf(conn.getHandlerSpan()).start();
        }
        try {
            int size = packet.calcPacketSize();
            if (size >= MySQLPacket.MAX_PACKET_SIZE) {
                packet.writeBigPackage(conn, size);
            } else {
                writeCommonPackage(conn);
            }
        } finally {
            if (span != null) {
                span.finish();
            }
        }

    }

    private void writeCommonPackage(MySQLConnection c) {
        ByteBuffer buffer = conn.allocate();
        try {
            BufferUtil.writeUB3(buffer, packet.calcPacketSize());
            buffer.put(packet.packetId);
            buffer.put(packet.getCommand());
            buffer = conn.writeToBuffer(packet.getArg(), buffer);
            conn.write(buffer);
        } catch (java.nio.BufferOverflowException e1) {
            buffer = conn.checkWriteBuffer(buffer, MySQLPacket.PACKET_HEADER_SIZE + packet.calcPacketSize(), false);
            BufferUtil.writeUB3(buffer, packet.calcPacketSize());
            buffer.put(packet.packetId);
            buffer.put(packet.getCommand());
            buffer = conn.writeToBuffer(packet.getArg(), buffer);
            conn.write(buffer);
        }
    }
}
