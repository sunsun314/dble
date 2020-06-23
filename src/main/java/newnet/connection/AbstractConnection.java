package newnet.connection;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.TimeUtil;
import newcommon.service.AbstractService;
import newcommon.service.AuthResultInfo;
import newcommon.service.Service;
import newnet.IOProcessor;
import newnet.SocketWR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by szf on 2020/6/15.
 */
public abstract class AbstractConnection implements Connection {

    protected static final Logger LOGGER = LoggerFactory.getLogger(com.actiontech.dble.net.AbstractConnection.class);

    protected final NetworkChannel channel;

    private final SocketWR socketWR;

    protected volatile boolean isClosed = false;

    private volatile AbstractService service;

    protected IOProcessor processor;

    protected long id;

    protected volatile ByteBuffer readBuffer;

    //写出队列，由NIOSokecetWR写入写出
    protected final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();

    private volatile boolean flowControlled;

    //连接设置
    protected int readBufferChunk;
    protected int maxPacketSize;

    //统计值先不值得多管
    protected long startupTime;
    protected long lastReadTime;
    protected long lastWriteTime;
    protected long netInBytes;
    protected long netOutBytes;

    protected long lastLargeMessageTime;


    public AbstractConnection(NetworkChannel channel, SocketWR socketWR) {
        this.channel = channel;
        this.socketWR = socketWR;
        this.startupTime = TimeUtil.currentTimeMillis();
        this.lastReadTime = startupTime;
        this.lastWriteTime = startupTime;
    }


    public void onReadData(int got) throws IOException {
        if (isClosed) {
            return;
        }

        lastReadTime = TimeUtil.currentTimeMillis();
        if (lastReadTime == lastWriteTime) {
            lastWriteTime--;
        }
        if (got < 0) {
            this.close("stream closed");
            return;
        } else if (got == 0 && !this.channel.isOpen()) {
            this.close("stream closed");
            return;
        }
        netInBytes += got;

        service.handle(readBuffer);
    }


    public void close(String reason) {

    }

    public void close(Exception exception) {

    }


    public void compactReadBuffer(ByteBuffer buffer, int offset) {
        if (buffer == null) {
            return;
        }
        buffer.limit(buffer.position());
        buffer.position(offset);
        this.readBuffer = buffer.compact();
    }

    public void ensureFreeSpaceOfReadBuffer(ByteBuffer buffer,
                                            int offset, final int pkgLength) {
        if (buffer.capacity() < pkgLength) {
            ByteBuffer newBuffer = processor.getBufferPool().allocate(pkgLength);
            lastLargeMessageTime = TimeUtil.currentTimeMillis();
            buffer.position(offset);
            newBuffer.put(buffer);
            readBuffer = newBuffer;
            recycle(buffer);
        } else {
            if (offset != 0) {
                // compact bytebuffer only
                compactReadBuffer(buffer, offset);
            } else {
                throw new RuntimeException(" not enough space");
            }
        }
    }

    public void readReachEnd() {
        // if cur buffer is temper none direct byte buffer and not
        // received large message in recent 30 seconds
        // then change to direct buffer for performance
        if (readBuffer != null && !readBuffer.isDirect() &&
                lastLargeMessageTime < lastReadTime - 30 * 1000L) {  // used temp heap
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("change to direct con read buffer ,cur temp buf size :" + readBuffer.capacity());
            }
            recycle(readBuffer);
            readBuffer = processor.getBufferPool().allocate(readBufferChunk);
        } else {
            if (readBuffer != null) {
                readBuffer.clear();
            }
        }
    }


    public void setSocketParams(boolean isFrontChannel) throws IOException {
        SystemConfig system = SystemConfig.getInstance();
        int soRcvBuf;
        int soSndBuf;
        int soNoDelay;
        if (isFrontChannel) {
            soRcvBuf = system.getFrontSocketSoRcvbuf();
            soSndBuf = system.getFrontSocketSoSndbuf();
            soNoDelay = system.getFrontSocketNoDelay();
        } else {
            soRcvBuf = system.getBackSocketSoRcvbuf();
            soSndBuf = system.getBackSocketSoSndbuf();
            soNoDelay = system.getBackSocketNoDelay();
        }

        channel.setOption(StandardSocketOptions.SO_RCVBUF, soRcvBuf);
        channel.setOption(StandardSocketOptions.SO_SNDBUF, soSndBuf);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, soNoDelay == 1);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        /*todo 需要对于这些参数进行区分，到底应该是什么级别的参数
        this.setMaxPacketSize(system.getMaxPacketSize());
        this.setIdleTimeout(system.getIdleTimeout());
        this.initCharacterSet(system.getCharset());
        this.setReadBufferChunk(soRcvBuf);
        */
    }

    public final void recycle(ByteBuffer buffer) {
        this.processor.getBufferPool().recycle(buffer);
    }

    public final long getId() {
        return id;
    }


    public Service getService() {
        return service;
    }

    public abstract void setConnProperties(AuthResultInfo info);

    public ByteBuffer allocate() {
        int size = this.processor.getBufferPool().getChunkSize();
        return this.processor.getBufferPool().allocate(size);
    }

    public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        int offset = 0;
        int length = src.length;
        int remaining = buffer.remaining();
        while (length > 0) {
            if (remaining >= length) {
                buffer.put(src, offset, length);
                break;
            } else {
                buffer.put(src, offset, remaining);
                writePart(buffer);
                buffer = allocate();
                offset += remaining;
                length -= remaining;
                remaining = buffer.remaining();
            }
        }
        return buffer;
    }

    public void writePart(ByteBuffer buffer) {
        write(buffer);
    }

    public void write(byte[] data) {

    }

    public void write(ByteBuffer buffer) {
        if (isClosed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("it will not write because of closed " + this);
            }
            if (buffer != null) {
                recycle(buffer);
            }
            this.cleanup();
            return;
        }

        //这个地方不在connection层进行处理，而是由service层进行处理
       /* if (isSupportCompress()) {
            ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
            writeQueue.offer(newBuffer);
        } else {*/
        writeQueue.offer(buffer);

        // if ansyn write finished event got lock before me ,then writing
        // flag is set false but not start a write request
        // so we check again
        try {
            this.socketWR.doNextWriteCheck();
        } catch (Exception e) {
            LOGGER.info("write err:", e);
            this.close("write err:" + e);
        }
    }

    private void write() {

    }

    public boolean isClosed() {
        return isClosed;
    }

    public ByteBuffer findReadBuffer() {
        if (readBuffer == null) {
            readBuffer = processor.getBufferPool().allocate(processor.getBufferPool().getChunkSize());
        }
        return readBuffer;
    }


    protected synchronized void cleanup() {

        if (readBuffer != null) {
            this.recycle(readBuffer);
            this.readBuffer = null;
        }

        /** todo : 压缩和解压的故事不应该在这网络级别进行处理
         if (!decompressUnfinishedDataQueue.isEmpty()) {
         decompressUnfinishedDataQueue.clear();
         }

         if (!compressUnfinishedDataQueue.isEmpty()) {
         compressUnfinishedDataQueue.clear();
         }**/
        ByteBuffer buffer;
        while ((buffer = writeQueue.poll()) != null) {
            recycle(buffer);
        }
    }

    public void writeStatistics(int netOutBytes) {
        this.netOutBytes += netOutBytes;
        processor.addNetOutBytes(netOutBytes);
        lastWriteTime = TimeUtil.currentTimeMillis();
    }

    public NetworkChannel getChannel() {
        return channel;
    }

    public SocketWR getSocketWR() {
        return socketWR;
    }

    public void register() throws IOException {
        this.service.register();
    }

    public Queue getWriteQueue() {
        return writeQueue;
    }

    public void asyncRead() throws IOException {
        this.socketWR.asyncRead();
    }

    public void doNextWriteCheck() throws IOException {
        this.socketWR.doNextWriteCheck();
    }

    public void setProcessor(IOProcessor processor) {
        this.processor = processor;
    }

    public void setId(long id) {
        this.id = id;
    }

    /**
     * 流量控制相关方法，暂时不管，后续再进行处理
     * ----------------------------------------------------------------------------------------------------------------------
     */

    public boolean isFlowControlled() {
        return flowControlled;
    }

    public void setFlowControlled(boolean flowControlled) {
        this.flowControlled = flowControlled;
    }

    public abstract void startFlowControl(BackendConnection bcon);

    public abstract void stopFlowControl();
}
