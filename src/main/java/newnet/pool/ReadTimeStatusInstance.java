package newnet.pool;

/**
 * Created by szf on 2020/6/29.
 */
public interface ReadTimeStatusInstance {

    public boolean isReadInstance();

    public boolean isDisabled();

    public boolean isAutocommitSynced();

    public boolean isIsolationSynced();

    public boolean isAlive();
}
