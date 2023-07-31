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

package com.ververica.cdc.connectors.sqlserver.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.connectors.base.catalog.AbstractJdbcCatalog;
import com.ververica.cdc.connectors.sqlserver.source.dialect.SqlServerSchema;
import com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerTypeUtils;
import com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerJdbcConfiguration;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.TABLE_NAME;

/** Sql Server Catalog. */
public class SqlServerCatalog extends AbstractJdbcCatalog {
    private final Configuration configuration;
    private final SqlServerJdbcConfiguration jdbcConfiguration;
    private final String schemaName;

    private final SqlServerSchema sqlServerSchema;

    public SqlServerCatalog(
            String name,
            String defaultDatabase,
            Configuration configuration,
            SqlServerJdbcConfiguration jdbcConfiguration) {
        super(name, defaultDatabase);
        this.configuration = configuration;
        this.schemaName = configuration.getString(SCHEMA_NAME.key());
        this.sqlServerSchema = new SqlServerSchema();
        this.jdbcConfiguration = jdbcConfiguration;
    }

    @Override
    public JdbcConnection getJdbcConnection() {
        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(jdbcConfiguration);
        SqlServerValueConverters valueConverters =
                new SqlServerValueConverters(
                        connectorConfig.getDecimalMode(),
                        connectorConfig.getTemporalPrecisionMode(),
                        connectorConfig.binaryHandlingMode());
        return new SqlServerConnection(
                connectorConfig.getJdbcConfig(),
                valueConverters,
                connectorConfig.getSkippedOperations(),
                connectorConfig.useSingleDatabase());
    }

    @Override
    public String getUrl() {
        return ((SqlServerConnection) connection).connectionString();
    }

    @Override
    public DynamicTableFactory getDynamicTableFactory() {
        return new SqlServerTableFactory();
    }

    @Override
    public Map<String, String> getTableOptions(ObjectPath tablePath) {
        Map<String, String> options =
                configuration.filter(key -> !key.equalsIgnoreCase(SCHEMA_NAME.key())).asMap();
        options.put(TABLE_NAME.key(), schemaName + "." + tablePath.getObjectName());
        return options;
    }

    @Override
    public Schema createTableSchema(String databaseName, String tableName) throws SQLException {
        Table table =
                sqlServerSchema
                        .getTableSchema(
                                connection, new TableId(databaseName, schemaName, tableName))
                        .getTable();
        List<Column> columns = table.columns();
        Schema.Builder builder = Schema.newBuilder();
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
        return SqlServerTypeUtils.fromDbzColumn(column);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return connection.readAllCatalogNames().stream()
                    .map(String::toUpperCase)
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new CatalogException("获取数据库列表异常!!!", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        try {
            return connection.readTableNames(databaseName, schemaName, null, new String[] {"TABLE"})
                    .stream()
                    .map(tableId -> tableId.table().toUpperCase())
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
