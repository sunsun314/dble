/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.common.net.aio;

import com.actiontech.dble.common.util.TimeUtil;
import com.actiontech.dble.common.net.AbstractConnection;
import com.actiontech.dble.common.net.SocketWR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class AIOSocketWR extends SocketWR {
    private static final Logger LOGGER = LoggerFactory.getLogger(AIOSocketWR.class);
    private static final AIOReadHandler AIO_READ_HANDLER = new AIOReadHandler();
    private static final AIOWriteHandler AIO_WRITE_HANDLER = new AIOWriteHandler();
    private final AsynchronousSocketChannel channel;
    protected final AbstractConnection con;
    protected final AtomicBoolean writing = new AtomicBoolean(false);


    public AIOSocketWR(AbstractConnection conn) {
        channel = (AsynchronousSocketChannel) conn.getChannel();
        this.con = conn;
    }

    @Override
    public void asyncRead() {
        ByteBuffer theBuffer = con.getReadBuffer();
        if (theBuffer == null) {
            theBuffer = con.getProcessor().getBufferPool().allocate(con.getProcessor().getBufferPool().getChunkSize());
            con.setReadBuffer(theBuffer);
            channel.read(theBuffer, this, AIO_READ_HANDLER);

        } else if (theBuffer.hasRemaining()) {
            channel.read(theBuffer, this, AIO_READ_HANDLER);
        } else {
            throw new java.lang.IllegalArgumentException("full buffer to read ");
        }

    }

    private void asyncWrite(final ByteBuffer buffer) {

        buffer.flip();
        this.channel.write(buffer, this, AIO_WRITE_HANDLER);


    }

    /**
     * return true ,means no more data
     *
     * @return
     */
    private boolean write0() {
        if (!writing.compareAndSet(false, true)) {
            return false;
        }
        ByteBuffer theBuffer = con.getWriteBuffer();
        if (theBuffer == null || !theBuffer.hasRemaining()) { // writeFinished,if buffer not NULL,recycle
            if (theBuffer != null) {
                con.recycle(theBuffer);
                con.setWriteBuffer(null);
            }
            // poll again
            ByteBuffer buffer = con.getWriteQueue().poll();
            // more data
            if (buffer != null) {
                if (buffer.limit() == 0) {
                    con.recycle(buffer);
                    con.setWriteBuffer(null);
                    con.close("quit cmd");
                    writing.set(false);
                    return true;
                } else {
                    con.setWriteBuffer(buffer);
                    asyncWrite(buffer);
                    return false;
                }
            } else {
                // no buffer
                writing.set(false);
                return true;
            }
        } else {
            theBuffer.compact();
            asyncWrite(theBuffer);
            return false;
        }

    }

    protected void onWriteFinished(int result) {

        con.setNetOutBytes(result + con.getNetOutBytes());
        con.getProcessor().addNetOutBytes(result);
        con.setLastWriteTime(TimeUtil.currentTimeMillis());
        boolean noMoreData = this.write0();
        if (noMoreData) {
            this.doNextWriteCheck();
        }

    }

    public void doNextWriteCheck() {

        boolean noMoreData = false;
        noMoreData = this.write0();
        if (noMoreData && !con.getWriteQueue().isEmpty()) {
            this.write0();
        }


    }

    public boolean registerWrite(ByteBuffer buffer) {
        con.setWriteBuffer(buffer);
        buffer.flip();
        try {
            write0();
        } catch (Exception e) {
            //SLB no exception in AIO
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("caught err:", e);
            }
            con.close("err:" + e);
            return false;
        }
        return true;
    }

    @Override
    public void disableRead() {

    }

    @Override
    public void enableRead() {

    }
}


