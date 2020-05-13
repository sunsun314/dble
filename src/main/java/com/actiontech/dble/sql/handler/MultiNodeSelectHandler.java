/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.sql.handler;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.assistant.backend.BackendConnection;
import com.actiontech.dble.assistant.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.sql.route.complex.builder.BaseHandlerBuilder;
import com.actiontech.dble.sql.handler.query.impl.OutputHandler;
import com.actiontech.dble.sql.handler.transaction.AutoTxOperation;
import com.actiontech.dble.sql.handler.util.ArrayMinHeap;
import com.actiontech.dble.sql.handler.util.HandlerTool;
import com.actiontech.dble.sql.handler.util.HeapItem;
import com.actiontech.dble.sql.handler.util.RowDataComparator;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.sql.route.complex.plan.Order;
import com.actiontech.dble.sql.route.complex.plan.common.item.ItemField;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.service.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MultiNodeSelectHandler extends MultiNodeQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeSelectHandler.class);
    private final int queueSize;
    private Map<BackendConnection, BlockingQueue<HeapItem>> queues;
    private RowDataComparator rowComparator;
    private OutputHandler outputHandler;
    private volatile boolean noNeedRows = false;

    public MultiNodeSelectHandler(RouteResultset rrs, NonBlockingSession session) {
        super(rrs, session);
        this.queueSize = DbleServer.getInstance().getConfig().getSystem().getMergeQueueSize();
        this.queues = new ConcurrentHashMap<>();
        outputHandler = new OutputHandler(BaseHandlerBuilder.getSequenceId(), session);
    }

    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + conn);
        }
        if (executeResponse) {
            String reason = "unexpected okResponse";
            LOGGER.info(reason);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        queues.put(conn, new LinkedBlockingQueue<>(queueSize));
        lock.lock();
        try {
            if (isFail()) {
                if (decrementToZero(conn)) {
                    session.resetMultiStatementStatus();
                    handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, false);
                }
            } else {
                if (!fieldsReturned) {
                    fieldsReturned = true;
                    mergeFieldEof(fields, conn);
                }
                if (decrementToZero(conn)) {
                    startOwnThread();
                }
            }
        } catch (Exception e) {
            cleanBuffer();
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, BackendConnection conn) {
        BlockingQueue<HeapItem> queue = queues.get(conn);
        if (queue == null)
            return;
        try {
            queue.put(HeapItem.nullItem());
        } catch (InterruptedException e) {
            LOGGER.info("rowEofResponse error", e);
        }
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, BackendConnection conn) {
        if (errorResponse.get() || noNeedRows) {
            return true;
        }
        BlockingQueue<HeapItem> queue = queues.get(conn);
        if (queue == null)
            return true;
        RowDataPacket rp = new RowDataPacket(fieldCount);
        rp.read(row);
        HeapItem item = new HeapItem(row, rp, (MySQLConnection) conn);
        try {
            queue.put(item);
        } catch (InterruptedException e) {
            LOGGER.info("rowResponse error", e);
        }
        return false;
    }

    @Override
    public void writeQueueAvailable() {
    }

    private void mergeFieldEof(List<byte[]> fields, BackendConnection conn) throws IOException {
        fieldCount = fields.size();
        List<FieldPacket> fieldPackets = new ArrayList<>();
        for (byte[] field : fields) {
            FieldPacket fieldPacket = new FieldPacket();
            fieldPacket.read(field);
            if (rrs.getSchema() != null) {
                fieldPacket.setDb(rrs.getSchema().getBytes());
            }
            if (rrs.getTableAlias() != null) {
                fieldPacket.setTable(rrs.getTableAlias().getBytes());
            }
            if (rrs.getTable() != null) {
                fieldPacket.setOrgTable(rrs.getTable().getBytes());
            }
            fieldPackets.add(fieldPacket);
        }
        List<Order> orderBys = new ArrayList<>();
        for (String groupBy : rrs.getGroupByCols()) {
            ItemField itemField = new ItemField(rrs.getSchema(), rrs.getTableAlias(), groupBy);
            orderBys.add(new Order(itemField));
        }
        rowComparator = new RowDataComparator(HandlerTool.createFields(fieldPackets), orderBys);
        outputHandler.fieldEofResponse(null, null, fieldPackets, null, false, conn);
    }

    private void startOwnThread() {
        DbleServer.getInstance().getComplexQueryExecutor().execute(() -> ownThreadJob());
    }

    private void ownThreadJob() {
        try {
            ArrayMinHeap<HeapItem> heap = new ArrayMinHeap<>((o1, o2) -> {
                RowDataPacket row1 = o1.getRowPacket();
                RowDataPacket row2 = o2.getRowPacket();
                if (row1 == null || row2 == null) {
                    if (row1 == row2)
                        return 0;
                    if (row1 == null)
                        return -1;
                    return 1;
                }
                return rowComparator.compare(row1, row2);
            });
            // init heap
            for (Map.Entry<BackendConnection, BlockingQueue<HeapItem>> entry : queues.entrySet()) {
                HeapItem firstItem = entry.getValue().take();
                heap.add(firstItem);
            }
            while (!heap.isEmpty()) {
                if (isFail())
                    return;
                HeapItem top = heap.peak();
                if (top.isNullItem()) {
                    heap.poll();
                } else {
                    BlockingQueue<HeapItem> topItemQueue = queues.get(top.getIndex());
                    HeapItem item = topItemQueue.take();
                    heap.replaceTop(item);
                    //limit
                    this.selectRows++;
                    if (rrs.getLimitSize() >= 0) {
                        if (selectRows <= rrs.getLimitStart()) {
                            continue;
                        } else if (selectRows > (rrs.getLimitStart() < 0 ? 0 : rrs.getLimitStart()) + rrs.getLimitSize()) {
                            noNeedRows = true;
                            while (!heap.isEmpty()) {
                                HeapItem itemToDiscard = heap.poll();
                                if (!itemToDiscard.isNullItem()) {
                                    BlockingQueue<HeapItem> discardQueue = queues.get(itemToDiscard.getIndex());
                                    while (true) {
                                        if (discardQueue.take().isNullItem() || isFail()) {
                                            break;
                                        }
                                    }
                                }
                            }
                            continue;
                        }
                    }
                    outputHandler.rowResponse(top.getRowData(), top.getRowPacket(), false, top.getIndex());
                }
            }
            Iterator<Map.Entry<BackendConnection, BlockingQueue<HeapItem>>> iterator = this.queues.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<BackendConnection, BlockingQueue<HeapItem>> entry = iterator.next();
                entry.getValue().clear();
                session.releaseConnectionIfSafe(entry.getKey(), false);
                iterator.remove();
            }
            doSqlStat();
            outputHandler.rowEofResponse(null, false, null);
        } catch (Exception e) {
            String msg = "Merge thread error, " + e.getLocalizedMessage();
            LOGGER.info(msg, e);
            session.onQueryError(msg.getBytes());
        }
    }
}
