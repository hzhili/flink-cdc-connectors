/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.catalog;

import com.ververica.cdc.connectors.oracle.table.OracleReadableMetaData;
import io.debezium.connector.oracle.OracleConnectorConfig;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.connectors.base.catalog.AbstractJdbcCatalog;
import com.ververica.cdc.connectors.oracle.source.utils.OracleTypeUtils;
import com.ververica.cdc.connectors.oracle.table.OracleTableSourceFactory;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.base.catalog.JdbcCatalogOptions.ENABLE_METADATA_COLUMN;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.TABLE_NAME;

/**
 * Oracle catalog implements,extends {@link AbstractJdbcCatalog }.
 */
public class OracleCatalog extends AbstractJdbcCatalog {
    private static final List<String> ORACLE_SYSTEM_USER =
            Arrays.asList("SYS", "SYSTEM", "SYSAUX", "SYSMAN", "DBSNMP", "OUTLN", "APPQOSSYS");

    public static final String ALL_DATABASES;
    private static final int FETCH_SIZE = 1024;
    private final Configuration configuration;
    private final JdbcConfiguration jdbcConfiguration;
    private final Boolean showMetadataCol;

    static {
        StringBuilder query = new StringBuilder("SELECT USERNAME FROM DBA_USERS WHERE ACCOUNT_STATUS='OPEN' "
                + "AND USERNAME NOT IN (");
        for (Iterator<String> i = OracleConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext(); ) {
            String excludedSchema = i.next();
            query.append('\'').append(excludedSchema.toUpperCase()).append('\'');
            if (i.hasNext()) {
                query.append(',');
            }
        }
        ALL_DATABASES = query.append(')').toString();
    }

    public OracleCatalog(
            String name,
            String defaultDatabase,
            Configuration configuration,
            JdbcConfiguration jdbcConfiguration) {
        super(name, defaultDatabase);
        this.configuration = configuration;
        this.jdbcConfiguration = jdbcConfiguration;
        this.showMetadataCol = configuration.getBoolean(ENABLE_METADATA_COLUMN.key());
    }

    @Override
    public JdbcConnection getJdbcConnection() {
        return new OracleConnection(jdbcConfiguration);
    }

    @Override
    public String getUrl() {
        return connection.connectionString(OracleConnection.connectionString(jdbcConfiguration));
    }

    @Override
    public DynamicTableFactory getDynamicTableFactory() {
        return new OracleTableSourceFactory();
    }

    @Override
    public Map<String, String> getTableOptions(ObjectPath tablePath) {
        Map<String, String> options = configuration.asMap();
        options.put(TABLE_NAME.key(), tablePath.getObjectName().toUpperCase());
        return options;
    }

    @Override
    public Schema createTableSchema(String databaseName, String tableName) throws SQLException {
        Table table =
                ((OracleConnection) connection)
                        .readTableSchema(
                                jdbcConfiguration.getDatabase().toUpperCase(),
                                databaseName.toUpperCase(),
                                tableName.toUpperCase(),
                                null)
                        .forTable(
                                new TableId(
                                        jdbcConfiguration.getDatabase().toUpperCase(),
                                        databaseName.toUpperCase(),
                                        tableName.toUpperCase()));
        List<Column> columns = table.columns();
        Schema.Builder builder = Schema.newBuilder();
        if (showMetadataCol) {
            builder.columnByMetadata(
                            "metadata_database_name",
                            OracleReadableMetaData.DATABASE_NAME.getDataType(),
                            OracleReadableMetaData.DATABASE_NAME.getKey())
                    .columnByMetadata(
                            "metadata_schema_name",
                            OracleReadableMetaData.SCHEMA_NAME.getDataType(),
                            OracleReadableMetaData.SCHEMA_NAME.getKey())
                    .columnByMetadata(
                            "metadata_table_name",
                            OracleReadableMetaData.TABLE_NAME.getDataType(),
                            OracleReadableMetaData.TABLE_NAME.getKey())
                    .columnByMetadata(
                            "metadata_op_ts",
                            OracleReadableMetaData.OP_TS.getDataType(),
                            OracleReadableMetaData.OP_TS.getKey())
                    .columnByMetadata(
                            "metadata_op",
                            OracleReadableMetaData.OP.getDataType(),
                            OracleReadableMetaData.OP.getKey());
        }
        columns.forEach(
                column -> {
                    builder.column(column.name(), convertColumnType(column))
                            .withComment(column.comment());
                });
        if (table.primaryKeyColumnNames().size() > 0 || !table.primaryKeyColumnNames().isEmpty()) {
            builder.primaryKey(table.primaryKeyColumnNames());
        }
        return builder.build();
    }

    @Override
    public DataType convertColumnType(Column column) {
        return OracleTypeUtils.fromDbzColumn(column);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return connection.queryAndMap(
                    ALL_DATABASES,
                    rs -> {
                        List<String> databases = new ArrayList<>();
                        rs.setFetchSize(FETCH_SIZE);
                        while (rs.next()) {
                            databases.add(rs.getString("USERNAME"));
                        }
                        return databases.parallelStream().sorted().collect(Collectors.toList());
                    });
        } catch (SQLException e) {
            throw new CatalogException("获取数据库列表异常!!!", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        String queryAllTableSql = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER=?";
        try {
            return connection.prepareQueryAndMap(
                    queryAllTableSql,
                    ps -> {
                        ps.setString(1, databaseName.toUpperCase());
                        ps.setFetchSize(FETCH_SIZE);
                    },
                    rs -> {
                        List<String> tables = new ArrayList<>();
                        while (rs.next()) {
                            tables.add(rs.getString(1));
                        }
                        return tables.stream().sorted().collect(Collectors.toList());
                    });
        } catch (SQLException e) {
            throw new CatalogException("获取表列表时出现异常!!!", e);
        }
    }
}
