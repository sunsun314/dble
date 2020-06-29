package newnet.connection;



/**
 * Created by szf on 2020/6/15.
 */
public interface Connection {


    /**
     * Connection forced to close function
     * would be called by IO error .....
     * @param reason
     */
    void close(String reason);

    void businessClose(String reason);
}
