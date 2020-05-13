/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sql.route.simple.impl;

import com.actiontech.dble.common.cache.LayerCachePool;
import com.actiontech.dble.common.config.model.SchemaConfig;
import com.actiontech.dble.sql.route.simple.parser.druid.DruidParserFactory;
import com.actiontech.dble.sql.route.simple.RouteResultset;
import com.actiontech.dble.sql.route.simple.parser.druid.DruidParser;
import com.actiontech.dble.sql.route.simple.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.sql.route.simple.util.RouterUtil;
import com.actiontech.dble.service.server.ServerConnection;
import com.actiontech.dble.singleton.TraceManager;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.opentracing.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

public class DefaultRouteStrategy extends AbstractRouteStrategy {

    public static final Logger LOGGER = LoggerFactory.getLogger(DefaultRouteStrategy.class);


    public SQLStatement parserSQL(String originSql, ServerConnection c) throws SQLSyntaxErrorException {
        SQLStatementParser parser;
        parser = new MySqlStatementParser(originSql);
        try {
            return parser.parseStatement(true);
        } catch (Exception t) {
            LOGGER.info("routeNormalSqlWithAST", t);
            if (t.getMessage() != null) {
                throw new SQLSyntaxErrorException(t.getMessage());
            } else {
                throw new SQLSyntaxErrorException(t);
            }
        }
    }

    @Override
    public SQLStatement parserSQL(String originSql) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(originSql);

        /**
         * thrown SQL SyntaxError if parser error
         */
        try {
            List<SQLStatement> list = parser.parseStatementList();
            if (list.size() > 1) {
                throw new SQLSyntaxErrorException("MultiQueries is not supported,use single query instead ");
            }
            return list.get(0);
        } catch (Exception t) {
            LOGGER.info("routeNormalSqlWithAST", t);
            if (t.getMessage() != null) {
                throw new SQLSyntaxErrorException(t.getMessage());
            } else {
                throw new SQLSyntaxErrorException(t);
            }
        }
    }

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ServerConnection sc, LayerCachePool cachePool) throws SQLException {
        return this.route(schema, sqlType, origSQL, sc, cachePool, false);
    }


    @Override
    public RouteResultset routeNormalSqlWithAST(SchemaConfig schema,
                                                String originSql, RouteResultset rrs,
                                                LayerCachePool cachePool, ServerConnection sc, boolean isExplain) throws SQLException {
        Span span = TraceManager.getTracer().buildSpan("parse-sql").start();
        SQLStatement statement = null;
        try {
            statement = parserSQL(originSql, sc);
        } finally {
            span.finish();
        }
        if (sc.getSession2().getIsMultiStatement().get()) {
            originSql = statement.toString();
            rrs.setStatement(originSql);
            rrs.setSrcStatement(originSql);
        }
        sc.getSession2().endParse();
        DruidParser druidParser = DruidParserFactory.create(statement, rrs.getSqlType());
        return RouterUtil.routeFromParser(druidParser, schema, rrs, statement, originSql, cachePool, new ServerSchemaStatVisitor(), sc, null, isExplain);

    }

}
