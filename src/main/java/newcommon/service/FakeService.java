package newcommon.service;

import java.nio.Buffer;

/**
 * Created by szf on 2020/6/18.
 */
public class FakeService implements Service {

    @Override
    public void handle(Buffer dataBuffer) {

    }

    @Override
    public void execute() {
        return;
    }
}
