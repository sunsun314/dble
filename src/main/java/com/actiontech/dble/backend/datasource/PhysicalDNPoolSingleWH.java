package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DataHostConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Created by szf on 2019/10/17.
 */
public class PhysicalDNPoolSingleWH extends AbstractPhysicalDBPool {

    private PhysicalDatasource writeSource;

    private Map<String, PhysicalDatasource> allSourceMap;


    public PhysicalDNPoolSingleWH(String name, DataHostConfig conf, PhysicalDatasource[] writeSources, Map<Integer, PhysicalDatasource[]> readSources, Map<Integer, PhysicalDatasource[]> standbyReadSourcesMap, int balance) {
    }


    @Override
    PhysicalDatasource findDatasource(BackendConnection exitsCon) {
        return null;
    }

    @Override
    boolean isSlave(PhysicalDatasource ds) {
        return false;
    }

    @Override
    public PhysicalDatasource[] getSources() {
        return new PhysicalDatasource[0];
    }

    @Override
    public PhysicalDatasource getSource() {
        return null;
    }

    @Override
    public boolean isInitSuccess() {
        return false;
    }

    @Override
    public boolean switchSource(int newIndex, String reason) {
        return false;
    }

    @Override
    public int init(int index) {
        return 0;
    }

    @Override
    public int reloadInit(int index) {
        return 0;
    }

    @Override
    public void doHeartbeat() {

    }

    @Override
    public void heartbeatCheck(long ildCheckPeriod) {

    }

    @Override
    public void startHeartbeat() {

    }

    @Override
    public void stopHeartbeat() {

    }

    @Override
    public void clearDataSources(String reason) {

    }

    @Override
    public Collection<PhysicalDatasource> getAllDataSources() {
        return null;
    }

    @Override
    public Map<Integer, PhysicalDatasource[]> getStandbyReadSourcesMap() {
        return null;
    }

    @Override
    void getRWBalanceCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception {

    }

    @Override
    PhysicalDatasource getRWBalanceNode() {
        return null;
    }

    @Override
    PhysicalDatasource getReadNode() throws Exception {
        return null;
    }

    @Override
    boolean getReadCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception {
        return false;
    }

    @Override
    public boolean equalsBaseInfo(AbstractPhysicalDBPool pool) {
        return false;
    }

    @Override
    String[] getSchemas() {
        return new String[0];
    }

    @Override
    public void setSchemas(String[] mySchemas) {

    }

    @Override
    public DataHostConfig getDataHostConfig() {
        return null;
    }

    @Override
    PhysicalDatasource[] getWriteSources() {
        return new PhysicalDatasource[0];
    }

    @Override
    public int getActiveIndex() {
        return 0;
    }

    @Override
    public Map<Integer, PhysicalDatasource[]> getReadSources() {
        return null;
    }

    @Override
    public Map<Integer, PhysicalDatasource[]> getrReadSources() {
        return null;
    }

    @Override
    public void switchSourceIfNeed(PhysicalDatasource ds, String reason) {

    }

    @Override
    public PhysicalDatasource randomSelect(ArrayList<PhysicalDatasource> okSources) {
        return null;
    }

    @Override
    public int next(int i) {
        return 0;
    }
}
