package com.actiontech.dble.config.util;

import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.config.loader.zkprocess.entity.Schemas;

import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2019/10/23.
 */
public class SchemaWriteJob implements Runnable {
    private final Set<PhysicalDNPoolSingleWH> changeSet;
    private final Schemas schemas;
    private volatile boolean finish = false;
    private final ReentrantLock lock = new ReentrantLock();
    private Condition initiated = lock.newCondition();

    public SchemaWriteJob(Set<PhysicalDNPoolSingleWH> changeSet, Schemas schemas) {
        this.changeSet = changeSet;
        this.schemas = schemas;
    }

    @Override
    public void run() {


    }

    public void waitForWritingDone() {
        lock.lock();
        try {

        } finally {

        }
    }
}
