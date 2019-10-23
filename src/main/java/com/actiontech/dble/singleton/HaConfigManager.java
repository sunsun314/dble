package com.actiontech.dble.singleton;

import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.loader.zkprocess.entity.Schemas;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.xml.SchemasParseXmlImpl;
import com.actiontech.dble.config.util.SchemaWriteJob;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by szf on 2019/10/23.
 */
public class HaConfigManager {

    private static final HaConfigManager INSTANCE = new HaConfigManager();
    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;
    private static final String WRITEPATH = "schema.xml";
    private static Schemas schema;
    private final AtomicBoolean isWriting = new AtomicBoolean(false);
    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();
    private volatile SchemaWriteJob schemaWriteJob;
    private volatile Set<PhysicalDNPoolSingleWH> waitingSet = new HashSet<>();

    private HaConfigManager() {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlProcess);
    }

    public void init() {
        schema = this.parseSchemaXmlService.parseXmlToBean(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
    }


    public void updateConfDataHost(PhysicalDNPoolSingleWH physicalDNPoolSingleWH, boolean syncWriteConf) {
        SchemaWriteJob thisTimeJob = null;
        //check if there is one thread is writing
        if (isWriting.compareAndSet(false, true)) {
            adjustLock.writeLock().lock();
            try {
                waitingSet.add(physicalDNPoolSingleWH);
                schemaWriteJob = new SchemaWriteJob(waitingSet, schema);
                thisTimeJob = schemaWriteJob;
                waitingSet = new HashSet<>();
                new Thread(schemaWriteJob).start();
            } finally {
                adjustLock.writeLock().unlock();
            }
        } else {
            adjustLock.readLock().lock();
            try {
                thisTimeJob = schemaWriteJob;
                waitingSet.add(physicalDNPoolSingleWH);
            } finally {
                adjustLock.readLock().unlock();
            }
        }
        if (syncWriteConf) {
            //waitDone
            thisTimeJob.waitForWritingDone();
        }
    }


    public void triggerNext() {
        if (isWriting.compareAndSet(false, true)) {
            adjustLock.writeLock().lock();
            try {
                schemaWriteJob = new SchemaWriteJob(waitingSet, schema);
                waitingSet = new HashSet<>();
                new Thread(schemaWriteJob).start();
            } finally {
                adjustLock.writeLock().unlock();
            }
        }
    }


    public static HaConfigManager getInstance() {
        return INSTANCE;
    }
}
