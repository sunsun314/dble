package newnet.pool;

import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import newnet.IOProcessor;
import newnet.connection.PooledConnection;
import newnet.factory.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static newnet.connection.PooledConnection.*;


public class ConnectionPool extends PoolBase implements PooledConnectionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class);

    private final QueuedSequenceSynchronizer synchronizer;
    private final AtomicInteger waiters;
    private final CopyOnWriteArrayList<PooledConnection> allConnections;
    private final AtomicInteger totalConnections = new AtomicInteger();

    // evictor
    private final WeakReference<ClassLoader> factoryClassLoader;
    private volatile ConnectionPool.Evictor evictor = null;

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final PoolConfig poolConfig;

    public ConnectionPool(final DbInstanceConfig config, final ReadTimeStatusInstance instance, final PooledConnectionFactory factory) {
        super(config, instance, factory);

        // save the current TCCL (if any) to be used later by the evictor Thread
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            factoryClassLoader = null;
        } else {
            factoryClassLoader = new WeakReference<>(cl);
        }

        this.synchronizer = new QueuedSequenceSynchronizer();
        this.waiters = new AtomicInteger();
        this.allConnections = new CopyOnWriteArrayList<>();
        this.poolConfig = config.getPoolConfig();
    }

    public PooledConnection borrow(final String schema, long timeout, final TimeUnit timeUnit) throws InterruptedException {

        timeout = timeUnit.toNanos(timeout);
        final long startScan = System.nanoTime();
        final long originTimeout = timeout;
        PooledConnection createEntry = null;
        long startSeq;
        waiters.incrementAndGet();
        try {
            do {
                do {
                    startSeq = synchronizer.currentSequence();
                    for (PooledConnection entry : allConnections) {
                        if (entry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                            // if we might have stolen another thread's new connection, restart the add...
                            if (waiters.get() > 1 && createEntry == null) {
                                newPooledConnection(schema);
                            }
                            return entry;
                        }
                    }

                } while (startSeq < synchronizer.currentSequence());

                if (createEntry == null || createEntry.getState() != INITIAL) {
                    createEntry = newPooledConnection(schema);
                }

                timeout = originTimeout - (System.nanoTime() - startScan);
            } while (timeout > 10_000L && synchronizer.waitUntilSequenceExceeded(startSeq, timeout));
        } finally {
            waiters.decrementAndGet();
        }

        return null;
    }

    /**
     * Create a new pooled object.
     *
     * @param schema Key associated with new pooled object
     * @return The new, wrapped pooled object
     */
    private PooledConnection newPooledConnection(final String schema) {
        if (instance.isDisabled() || isClosed.get()) {
            return null;
        }

        if (totalConnections.incrementAndGet() <= config.getMaxCon()) {
            final PooledConnection conn = newConnection(schema, ConnectionPool.this);
            if (conn != null) {
                return conn;
            }
        }
        totalConnections.decrementAndGet();
        return null;
    }


    public void release(final PooledConnection conn) {
        if (poolConfig.getTestOnReturn()) {
            conn.synchronousTest();
        }

        conn.lazySet(STATE_NOT_IN_USE);
        synchronizer.signal();
    }

    private void fillPool() {
        final int idleCount = getCount(STATE_NOT_IN_USE, STATE_HEARTBEAT);
        final int connectionsToAdd = Math.min(config.getMaxCon() - totalConnections.get(), config.getMinCon() - idleCount) -
                (totalConnections.get() - idleCount);
        if (LOGGER.isDebugEnabled() && connectionsToAdd > 0) {
            LOGGER.debug("need add {}", connectionsToAdd);
        }
        for (int i = 0; i < connectionsToAdd; i++) {
            // newPooledConnection(schemas[i % schemas.length]);
            newPooledConnection(null);
        }
    }

    /**
     * Calculate the number of objects to test in a run of the idle object
     * evictor.
     *
     * @return The number of objects to test for validity
     */
    private int getNumTests() {
        final int totalIdle = getCount(STATE_NOT_IN_USE);
        final int numTests = poolConfig.getNumTestsPerEvictionRun();
        if (numTests >= 0) {
            return Math.min(numTests, totalIdle);
        }
        return (int) (Math.ceil(totalIdle / Math.abs((double) numTests)));
    }

    @Override
    public void onCreateSuccess(PooledConnection conn) {
        conn.setPoolRelated(this);
        allConnections.add(conn);
        if (poolConfig.getTestOnCreate()) {
            conn.synchronousTest();
        }
        conn.lazySet(STATE_NOT_IN_USE);
        synchronizer.signal();
    }

    @Override
    public void onCreateFail(PooledConnection conn, Throwable e) {
        LOGGER.warn("create connection fail " + e.getMessage());
        totalConnections.decrementAndGet();
        // conn can be null if newChannel crashed (eg SocketException("too many open files"))
        if (conn != null) {
            conn.closeWithoutRsp("create fail");
        }
    }

    @Override
    public void onHeartbeatSuccess(PooledConnection conn) {
        conn.lazySet(STATE_NOT_IN_USE);
        synchronizer.signal();
    }

    public int getCount(final int... states) {
        int count = 0;
        for (final PooledConnection conn : allConnections) {
            boolean allRight = true;
            for (int state : states) {
                if (conn.getState() != state) {
                    allRight = false;
                    break;
                }
            }
            if (allRight) {
                count++;
            }
        }
        return count;
    }

    public int getCount(String schema, final int... states) {
        int count = 0;
        for (final PooledConnection conn : allConnections) {
            if (!schema.equals(conn.getSchema())) {
                continue;
            }
            boolean allRight = true;
            for (int state : states) {
                if (conn.getState() != state) {
                    allRight = false;
                    break;
                }
            }
            if (allRight) {
                count++;
            }
        }
        return count;
    }

    public int size() {
        return allConnections.size();
    }

    public void close(final PooledConnection conn) {
        if (remove(conn)) {
            final int tc = totalConnections.decrementAndGet();
            if (tc < 0) {
                LOGGER.warn("{} - Unexpected value of totalConnections={}", config.getInstanceName(), tc);
            }
        }
    }

    private boolean remove(final PooledConnection PooledConnection) {
        //        if (!PooledConnection.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !PooledConnection.compareAndSet(STATE_RESERVED, STATE_REMOVED) &&
        //                !PooledConnection.compareAndSet(STATE_HEARTBEAT, STATE_REMOVED) && !isClosed.get()) {
        //            LOGGER.warn("Attempt to remove an object that was not borrowed or reserved: {}", PooledConnection);
        //            return false;
        //        }

        final boolean removed = allConnections.remove(PooledConnection);
        if (!removed) {
            LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", PooledConnection);
        }

        // synchronizer.signal();
        return removed;
    }


    public void softCloseAllConnections(final String closureReason) {
        while (totalConnections.get() > 0) {
            for (PooledConnection conn : allConnections) {
                if (conn.getState() == STATE_IN_USE) {
                    close(conn);
                    conn.setPoolDestroyedTime(System.currentTimeMillis());
                    IOProcessor.BACKENDS_OLD.add(conn);
                } else {
                    conn.close(closureReason);
                }
            }
        }
    }

    public void forceCloseAllConnection(final String closureReason) {
        while (totalConnections.get() > 0) {
            for (PooledConnection conn : allConnections) {
                if (conn.getState() == STATE_IN_USE) {
                    conn.closePooldestroyed(closureReason);
                } else {
                    conn.close(closureReason);
                }
            }
        }
    }


    /**
     * Closes the keyed object pool. Once the pool is closed
     */
    public void stop(final String closureReason) {
        stop(closureReason, false);
    }

    public void stop(final String closureReason, boolean closeFront) {
        if (isClosed.getAndSet(true)) {
            return;
        }

        stopEvictor();
        if (closeFront) {
            forceCloseAllConnection(closureReason);
        } else {
            softCloseAllConnections(closureReason);
        }
    }

    private void evict() {

        final ArrayList<PooledConnection> idleList = new ArrayList<>(allConnections.size());
        for (final PooledConnection entry : allConnections) {
            if (entry.getState() == STATE_NOT_IN_USE) {
                idleList.add(entry);
            }
        }

        int removable = idleList.size() - config.getMinCon();

        // Sort pool entries on lastAccessed
        idleList.sort(LAST_ACCESS_COMPARABLE);

        logPoolState("before cleanup ");
        for (PooledConnection conn : idleList) {
            if (removable > 0 && System.currentTimeMillis() - conn.getLastTime() > poolConfig.getIdleTimeout() &&
                    conn.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED)) {
                conn.close("connection has passed idleTimeout");
                removable--;
            } else if (poolConfig.getTestWhileIdle() && conn.compareAndSet(STATE_NOT_IN_USE, STATE_HEARTBEAT)) {
               /* ConnectionHeartBeatHandler heartBeatHandler = new ConnectionHeartBeatHandler(conn, false, this);
                heartBeatHandler.ping(poolConfig.getConnectionHeartbeatTimeout());*/
                conn.asynchronousTest();
            }
        }

    }

    public final int getThreadsAwaitingConnection() {
        return synchronizer.getQueueLength();
    }

    /**
     * <p>Starts the evictor with the given delay. If there is an evictor
     * running when this method is called, it is stopped and replaced with a
     * new evictor with the specified delay.</p>
     * <p>
     * <p>This method needs to be final, since it is called from a constructor.
     * See POOL-195.</p>
     */
    public void startEvictor() {
        if (evictor != null) {
            EvictionTimer.cancel(evictor, poolConfig.getEvictorShutdownTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        evictor = new Evictor();
        EvictionTimer.schedule(evictor, 0, poolConfig.getTimeBetweenEvictionRunsMillis());
    }

    /**
     * Stops the evictor.
     */
    public void stopEvictor() {
        EvictionTimer.cancel(evictor, poolConfig.getEvictorShutdownTimeoutMillis(), TimeUnit.MILLISECONDS);
        evictor = null;
    }

    /**
     * Log the current pool state at debug level.
     *
     * @param prefix an optional prefix to prepend the log message
     */
    private void logPoolState(String... prefix) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} db instance[{}] stats (total={}, active={}, idle={}, idleTest={} waiting={})", (prefix.length > 0 ? prefix[0] : ""), config.getInstanceName(),
                    allConnections.size() - getCount(STATE_REMOVED), getCount(STATE_IN_USE), getCount(STATE_NOT_IN_USE), getCount(STATE_HEARTBEAT), getThreadsAwaitingConnection());
        }
    }

    /**
     * The idle object evictor.
     */
    class Evictor implements Runnable {

        private ScheduledFuture<?> scheduledFuture;

        /**
         * Run pool maintenance.  Evict objects qualifying for eviction and then
         * ensure that the minimum number of idle instances are available.
         * Since the Timer that invokes Evictors is shared for all Pools but
         * pools may exist in different class loaders, the Evictor ensures that
         * any actions taken are under the class loader of the factory
         * associated with the pool.
         */
        @Override
        public void run() {

            if (!instance.isAlive()) {
                return;
            }

            final ClassLoader savedClassLoader =
                    Thread.currentThread().getContextClassLoader();
            try {
                if (factoryClassLoader != null) {
                    // Set the class loader for the factory
                    final ClassLoader cl = factoryClassLoader.get();
                    if (cl == null) {
                        // The pool has been dereferenced and the class loader
                        // GC'd. Cancel this timer so the pool can be GC'd as
                        // well.
                        cancel();
                        return;
                    }
                    Thread.currentThread().setContextClassLoader(cl);
                }

                // Evict from the pool
                evict();

                // Try to maintain minimum connections
                fillPool();
            } finally {
                // Restore the previous CCL
                Thread.currentThread().setContextClassLoader(savedClassLoader);
            }
        }

        /**
         * Sets the scheduled future.
         *
         * @param scheduledFuture the scheduled future.
         */
        void setScheduledFuture(final ScheduledFuture<?> scheduledFuture) {
            this.scheduledFuture = scheduledFuture;
        }

        /**
         * Cancels the scheduled future.
         */
        void cancel() {
            scheduledFuture.cancel(false);
        }
    }
}
