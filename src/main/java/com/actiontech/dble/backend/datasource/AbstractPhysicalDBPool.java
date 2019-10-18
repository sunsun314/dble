package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DataHostConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Created by szf on 2019/10/18.
 */
public abstract class AbstractPhysicalDBPool {

    public static final int BALANCE_NONE = 0;
    protected static final int BALANCE_ALL_BACK = 1;
    protected static final int BALANCE_ALL = 2;
    protected static final int BALANCE_ALL_READ = 3;

    protected String hostName;
    public static final int WEIGHT = 0;

    abstract PhysicalDatasource findDatasource(BackendConnection exitsCon);

    abstract boolean isSlave(PhysicalDatasource ds);

    public abstract PhysicalDatasource[] getSources();

    public abstract PhysicalDatasource getSource();

    public abstract boolean isInitSuccess();

    public abstract boolean switchSource(int newIndex, String reason);

    public abstract int init(int index);

    public abstract int reloadInit(int index);

    public abstract void doHeartbeat();

    public abstract void heartbeatCheck(long ildCheckPeriod);

    public abstract void startHeartbeat();

    public abstract void stopHeartbeat();

    public abstract void clearDataSources(String reason);

    public abstract Collection<PhysicalDatasource> getAllDataSources();

    public abstract Map<Integer, PhysicalDatasource[]> getStandbyReadSourcesMap();

    abstract void getRWBalanceCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception;

    abstract PhysicalDatasource getRWBalanceNode();

    abstract PhysicalDatasource getReadNode() throws Exception;

    abstract boolean getReadCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception;

    public abstract boolean equalsBaseInfo(AbstractPhysicalDBPool pool);

    abstract String[] getSchemas();

    public abstract void setSchemas(String[] mySchemas);

    public abstract DataHostConfig getDataHostConfig();

    abstract PhysicalDatasource[] getWriteSources();

    public String getHostName() {
        return hostName;
    }

    public abstract int getActiveIndex();

    public abstract Map<Integer, PhysicalDatasource[]> getReadSources();

    public abstract Map<Integer, PhysicalDatasource[]> getrReadSources();

    public abstract void switchSourceIfNeed(PhysicalDatasource ds, String reason);

    public abstract PhysicalDatasource randomSelect(ArrayList<PhysicalDatasource> okSources);

    public abstract int next(int i);
}
