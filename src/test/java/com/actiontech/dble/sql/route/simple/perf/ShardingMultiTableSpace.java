/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sql.route.simple.route.perf;

import com.actiontech.dble.SimpleCachePool;
import com.actiontech.dble.common.cache.LayerCachePool;
import com.actiontech.dble.common.config.loader.SchemaLoader;
import com.actiontech.dble.common.config.loader.xml.XMLSchemaLoader;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.route.factory.RouteStrategyFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * @author mycat
 */
public class ShardingMultiTableSpace {
    private SchemaConfig schema;
    private static int total = 1000000;
    protected LayerCachePool cachePool = new SimpleCachePool();

    public ShardingMultiTableSpace() throws InterruptedException {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile, true, null);
        schema = schemaLoader.getSchemas().get("cndb");
    }

    /**
     * @throws SQLNonTransientException
     */
    public void testTableSpace() throws SQLException {
        SchemaConfig schema = getSchema();
        String sql = "select id,member_id,gmt_create from offer where member_id in ('1','22','333','1124','4525')";
        for (int i = 0; i < total; i++) {
            RouteStrategyFactory.getRouteStrategy().route(schema, -1, sql, null, cachePool);
        }
    }

    protected SchemaConfig getSchema() {
        return schema;
    }

    public static void main(String[] args) throws Exception {
        ShardingMultiTableSpace test = new ShardingMultiTableSpace();
        System.currentTimeMillis();

        long start = System.currentTimeMillis();
        test.testTableSpace();
        long end = System.currentTimeMillis();
        System.out.println("take " + (end - start) + " ms. avg " + (end - start + 0.0) / total);
    }
}