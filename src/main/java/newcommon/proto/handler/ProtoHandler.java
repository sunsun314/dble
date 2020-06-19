package newcommon.proto.handler;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/6/16.
 */
public interface ProtoHandler {

    ProtoHandlerResult handle(ByteBuffer dataBuffer, int dataBufferOffset);
}
