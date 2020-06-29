package newservices.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import newcommon.proto.mysql.packet.*;
import newcommon.proto.mysql.util.PacketUtil;
import newservices.manager.ManagerService;
import newservices.mysqlsharding.GlobalCheckJob;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2019/12/25.
 */
public final class CheckGlobalConsistency {

    private Map<String, List<ConsistencyResult>> resultMap = new ConcurrentHashMap<>();
    private AtomicInteger counter;
    private final ManagerService service;
    private final List<GlobalCheckJob> globalCheckJobs;
    private final ReentrantLock lock = new ReentrantLock();


    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final Pattern PATTERN = Pattern.compile("check\\s+@@global(\\s+schema\\s*=\\s*\\'(([^'])+)\\'(\\s+and\\s+table\\s*=\\s*\\'([^']+)\\')?)?", Pattern.CASE_INSENSITIVE);

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TABLE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DISTINCT_CONSISTENCY_NUMBER", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ERROR_NODE_NUMBER", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private CheckGlobalConsistency(List<GlobalCheckJob> jobs, ManagerService service) {

        this.service = service;
        this.globalCheckJobs = jobs;
    }

    public static void execute(ManagerService service, String stmt) {
        Matcher ma = PATTERN.matcher(stmt);
        if (!ma.matches()) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The sql does not match: check @@global schema = ? and table = ?");
            return;
        }

        String schema = ma.group(2);
        String table = ma.group(5);

        Map<String, SchemaConfig> schemaConfigs = DbleServer.getInstance().getConfig().getSchemas();

        List<GlobalCheckJob> jobs = new ArrayList<>();
        CheckGlobalConsistency consistencyCheck = new CheckGlobalConsistency(jobs, service);
        if (schema != null) {
            SchemaConfig sc = schemaConfigs.get(schema);
            if (sc == null) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "schema must exists");
                return;
            } else {
                if (table != null) {
                    String[] tables = table.split(",");
                    for (String singleTable : tables) {
                        TableConfig config = sc.getTables().get(singleTable);
                        if (config == null || !config.isGlobalTable() || !config.isGlobalCheck()) {
                            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "tables must exist and must be global table with global check");
                            return;
                        } else {
                            jobs.add(new GlobalCheckJob(config, schema, consistencyCheck));
                        }
                    }
                } else {
                    for (Map.Entry<String, TableConfig> te : sc.getTables().entrySet()) {
                        TableConfig config = te.getValue();
                        if (config.isGlobalTable() && config.isGlobalCheck()) {
                            jobs.add(new GlobalCheckJob(config, schema, consistencyCheck));
                        }
                    }
                }
            }
        } else {
            for (Map.Entry<String, SchemaConfig> se : schemaConfigs.entrySet()) {
                for (Map.Entry<String, TableConfig> te : se.getValue().getTables().entrySet()) {
                    TableConfig config = te.getValue();
                    if (config.isGlobalTable() && config.isGlobalCheck()) {
                        jobs.add(new GlobalCheckJob(config, se.getKey(), consistencyCheck));
                    }
                }
            }
        }

        consistencyCheck.start();


    }

    private void response() {
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);
        // write rows
        byte packetId = EOF.getPacketId();
        for (Map.Entry<String, List<ConsistencyResult>> entry : resultMap.entrySet()) {
            for (ConsistencyResult cr : entry.getValue()) {
                RowDataPacket row = getRow(entry.getKey(), cr);
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, service, true);
        // post write
        service.write(buffer);
    }


    private void start() {
        counter = new AtomicInteger(globalCheckJobs.size());
        for (GlobalCheckJob job : globalCheckJobs) {
            job.checkGlobalTable();
        }
    }

    private RowDataPacket getRow(String schema, ConsistencyResult cr) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        String charset = service.getCharset().getResults();
        row.add(StringUtil.encode(schema, charset));
        row.add(StringUtil.encode(cr.table, charset));
        row.add(LongUtil.toBytes(cr.distinctNo));
        row.add(LongUtil.toBytes(cr.errorNo));
        return row;
    }


    public void collectResult(String schema, String table, int distinctNo, int errorNo) {
        lock.lock();
        try {
            List<ConsistencyResult> list = resultMap.get(schema);
            if (list == null) {
                list = Collections.synchronizedList(new ArrayList<ConsistencyResult>());
                resultMap.put(schema, list);
            }
            list.add(new ConsistencyResult(table, distinctNo, errorNo));
        } finally {
            lock.unlock();
        }
        if (counter.decrementAndGet() <= 0) {
            response();
        }
    }

    static class ConsistencyResult {
        final String table;
        final int distinctNo;
        final int errorNo;

        ConsistencyResult(String table, int distinctNo, int errorNo) {
            this.table = table;
            this.distinctNo = distinctNo;
            this.errorNo = errorNo;
        }

    }
}
