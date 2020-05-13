/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.service.handler;

import com.actiontech.dble.bootstrap.DbleServer;
import com.actiontech.dble.assistant.backend.mysql.CharsetUtil;
import com.actiontech.dble.common.config.ServerConfig;
import com.actiontech.dble.common.config.model.UserConfig;
import com.actiontech.dble.common.mysql.packet.FieldPacket;
import com.actiontech.dble.common.mysql.packet.RowDataPacket;
import com.actiontech.dble.service.ServerConnection;
import com.actiontech.dble.common.util.StringUtil;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


/**
 * MysqlInformationSchemaHandler
 * <p>
 * :SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA
 *
 * @author zhuam
 */
public final class MysqlInformationSchemaHandler {
    private MysqlInformationSchemaHandler() {
    }

    /**
     * @param c
     * @param fields
     */
    public static void handle(ServerConnection c, FieldPacket[] fields) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        Map<String, UserConfig> users = conf.getUsers();
        UserConfig user = users == null ? null : users.get(c.getUser());
        RowDataPacket[] rows = null;
        if (user != null) {
            TreeSet<String> schemaSet = new TreeSet<>();
            Set<String> schemaList = user.getSchemas();
            if (schemaList == null || schemaList.size() == 0) {
                schemaSet.addAll(conf.getSchemas().keySet());
            } else {
                schemaSet.addAll(schemaList);
            }

            rows = new RowDataPacket[schemaSet.size()];
            int index = 0;
            for (String name : schemaSet) {
                String charset = conf.getSystem().getCharset();
                RowDataPacket row = new RowDataPacket(fields.length);
                for (int j = 0; j < fields.length; j++) {
                    switch (StringUtil.decode(fields[j].getName(), c.getCharset().getResults())) {
                        case "SCHEMA_NAME":
                            row.add(StringUtil.encode(name, c.getCharset().getResults()));
                            break;
                        case "DEFAULT_CHARACTER_SET_NAME":
                            row.add(StringUtil.encode(charset, c.getCharset().getResults()));
                            break;
                        case "DEFAULT_COLLATION_NAME":
                            row.add(StringUtil.encode(CharsetUtil.getDefaultCollation(charset), c.getCharset().getResults()));
                            break;
                        default:
                            break;
                    }
                }
                rows[index++] = row;
            }
        }

        MysqlSystemSchemaHandler.doWrite(fields.length, fields, rows, c);
    }
}
