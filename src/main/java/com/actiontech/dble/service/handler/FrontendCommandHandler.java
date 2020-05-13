/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.service.handler;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.common.mysql.util.CharsetUtil;
import com.actiontech.dble.common.mysql.util.MySQLMessage;
import com.actiontech.dble.common.config.ErrorCode;
import com.actiontech.dble.common.net.FrontendConnection;
import com.actiontech.dble.common.net.NIOHandler;
import com.actiontech.dble.common.mysql.packet.ChangeUserPacket;
import com.actiontech.dble.common.mysql.packet.ErrorPacket;
import com.actiontech.dble.common.mysql.packet.MySQLPacket;
import com.actiontech.dble.service.server.NonBlockingSession;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.assistant.statistic.CommandCount;
import com.actiontech.dble.common.util.StringUtil;
import io.opentracing.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FrontendCommandHandler
 *
 * @author mycat
 */
public class FrontendCommandHandler implements NIOHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendCommandHandler.class);
    protected final FrontendConnection source;
    protected final CommandCount commands;
    private volatile byte[] dataTodo;
    private Queue<byte[]> blobDataQueue = new ConcurrentLinkedQueue<byte[]>();
    private AtomicBoolean isAuthSwitch = new AtomicBoolean(false);
    private volatile ChangeUserPacket changeUserPacket;

    FrontendCommandHandler(FrontendConnection source) {
        this.source = source;
        this.commands = source.getProcessor().getCommands();
    }

    @Override
    public void handle(byte[] data) {
        if (data.length - MySQLPacket.PACKET_HEADER_SIZE >= DbleServer.getInstance().getConfig().getSystem().getMaxPacketSize()) {
            MySQLMessage mm = new MySQLMessage(data);
            mm.readUB3();
            byte packetId = 0;
            if (source instanceof ServerConnection) {
                NonBlockingSession session = ((ServerConnection) source).getSession2();
                if (session != null) {
                    packetId = (byte) session.getPacketId().get();
                }
            } else {
                packetId = mm.read();
            }
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setErrNo(ErrorCode.ER_NET_PACKET_TOO_LARGE);
            errPacket.setMessage("Got a packet bigger than 'max_allowed_packet' bytes.".getBytes());
            //close the mysql connection if error occur
            errPacket.setPacketId(++packetId);
            errPacket.write(source);
            return;
        }
        if (source.getLoadDataInfileHandler() != null && source.getLoadDataInfileHandler().isStartLoadData()) {
            MySQLMessage mm = new MySQLMessage(data);
            int packetLength = mm.readUB3();
            if (packetLength + 4 == data.length) {
                source.loadDataInfileData(data);
            }
            return;
        }

        if (MySQLPacket.COM_STMT_SEND_LONG_DATA == data[4]) {
            commands.doStmtSendLongData();
            blobDataQueue.offer(data);
            return;
        } else if (MySQLPacket.COM_STMT_CLOSE == data[4]) {
            commands.doStmtClose();
            source.stmtClose(data);
            return;
        } else {
            dataTodo = data;
            if (MySQLPacket.COM_STMT_RESET == data[4]) {
                blobDataQueue.clear();
            }
        }
        if (source instanceof ServerConnection) {
            ((ServerConnection) source).getSession2().resetMultiStatementStatus();
        }
        DbleServer.getInstance().getFrontHandlerQueue().offer(this);
    }

    public void handle() {
        try {
            Span fs = TraceManager.popSpan(source, false);
            Span span = TraceManager.startSpan("query-execute", true, fs);
            TraceManager.setSpan(source, span);
            handleData(dataTodo);
        } catch (Throwable e) {
            String msg = e.getMessage();
            if (StringUtil.isEmpty(msg)) {
                LOGGER.info("Maybe occur a bug, please check it.", e);
                msg = e.toString();
            } else {
                LOGGER.info("There is an error you may need know.", e);
            }
            source.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
        }
    }

    protected void handleData(byte[] data) {
        source.startProcess();
        if (isAuthSwitch.compareAndSet(true, false)) {
            commands.doOther();
            source.changeUserAuthSwitch(data, changeUserPacket);
            return;
        }
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                source.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                source.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                source.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                source.close("quit cmd");
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                commands.doKill();
                source.kill(data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                source.stmtPrepare(data);
                break;
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                source.stmtReset(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                source.stmtExecute(data, blobDataQueue);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                source.heartbeat(data);
                break;
            case MySQLPacket.COM_SET_OPTION:
                commands.doOther();
                source.setOption(data);
                break;
            case MySQLPacket.COM_CHANGE_USER:
                commands.doOther();
                changeUserPacket = new ChangeUserPacket(source.getClientFlags(), CharsetUtil.getCollationIndex(source.getCharset().getCollation()));
                source.changeUser(data, changeUserPacket, isAuthSwitch);
                break;
            case MySQLPacket.COM_RESET_CONNECTION:
                commands.doOther();
                source.resetConnection();
                break;
            default:
                commands.doOther();
                source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }
}