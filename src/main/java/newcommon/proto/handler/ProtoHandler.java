package newcommon.proto.handler;

import newcommon.proto.mysql.packet.CharsetNames;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/6/16.
 */
public interface ProtoHandler {

    ProtoHandlerResult handle(ByteBuffer dataBuffer, int dataBufferOffset);

    String getSQL(byte[] data, CharsetNames charsetNames) throws UnsupportedEncodingException;
}
