package newnet;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.util.TimeUtil;
import newcommon.service.AuthSuccessInfo;
import newcommon.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;

/**
 * Created by szf on 2020/6/15.
 */
public abstract class AbstractConnection implements Connection {

    protected static final Logger LOGGER = LoggerFactory.getLogger(com.actiontech.dble.net.AbstractConnection.class);

    protected final NetworkChannel channel;

    private final SocketWR socketWR;

    protected volatile boolean isClosed = false;

    private volatile Service service;

    protected NIOProcessor processor;

    protected long id;

    protected long startupTime;
    protected long lastReadTime;
    protected long lastWriteTime;

    protected volatile ByteBuffer readBuffer;
    protected volatile ByteBuffer writeBuffer;


    //连接设置
    protected int readBufferChunk;
    protected int maxPacketSize;

    //统计值先不值得多管
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

        this.setMaxPacketSize(system.getMaxPacketSize());
        this.setIdleTimeout(system.getIdleTimeout());
        this.initCharacterSet(system.getCharset());
        this.setReadBufferChunk(soRcvBuf);
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

    public abstract void setConnProperties(AuthSuccessInfo info);

    public void write(byte[] data){
    }

    public void write(ByteBuffer byteBuffer){

    }
}
