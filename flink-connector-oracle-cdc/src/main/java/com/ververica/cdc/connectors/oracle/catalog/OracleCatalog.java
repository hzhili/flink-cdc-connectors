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

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.ververica.cdc.connectors.base.catalog.AbstractJdbcCatalog;
import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import com.ververica.cdc.connectors.oracle.source.utils.OracleTypeUtils;
import com.ververica.cdc.connectors.oracle.table.OracleTableSourceFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.shade.com.google.common.collect.Maps;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.TABLE_NAME;

/** Oracle catalog implements,extends {@link AbstractJdbcCatalog }. */
public class OracleCatalog extends AbstractJdbcCatalog {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleCatalog.class);
    private final Map<String, String> tableOptions;

    public OracleCatalog(
            String name,
            String defaultDatabase,
            JdbcSourceConfig jdbcSourceConfig,
            JdbcConnectionPoolFactory connectionPoolFactory,
            Map<String, String> tableOptions) {
        super(name, defaultDatabase, jdbcSourceConfig, connectionPoolFactory);

        this.tableOptions = tableOptions;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (JdbcConnection connection =
                new JdbcConnection(jdbcConfiguration, jdbcConnectionFactory, null, null)) {
            DatabaseMetaData metaData = connection.connection().getMetaData();
            ResultSet schemas = metaData.getSchemas();
            List<String> databases = new ArrayList<>();
            while (schemas.next()) {
                databases.add(schemas.getString(1));
            }
            return databases;
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Catalog %s failed get database.", getName()), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        try (JdbcConnection conn =
                new JdbcConnection(jdbcConfiguration, jdbcConnectionFactory, null, null)) {
            Set<TableId> tableIds =
                    conn.readTableNames(
                            null,
                            getSchemaName(conn, databaseName, null),
                            null,
                            new String[] {"TABLE"});
            return tableIds.stream().map(TableId::table).collect(Collectors.toList());
        } catch (SQLException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public DynamicTableFactory getDynamicTableFactory() {
        return new OracleTableSourceFactory();
    }

    @Override
    public String getSchemaName(JdbcConnection conn, String databaseName, String tableName) {
        return databaseName;
    }

    @Override
    public Map<String, String> getTableOptions(ObjectPath tablePath) {
        Map<String, String> options = Maps.newHashMap(tableOptions);
        options.put(TABLE_NAME.key(), tablePath.getObjectName());
        options.put(SCHEMA_NAME.key(), tablePath.getDatabaseName());
        return options;
    }

    @Override
    public Schema createTableSchema(String databaseName, String tableName) {
        try (JdbcConnection conn =
                new JdbcConnection(
                        JdbcConfiguration.adapt(jdbcSourceConfig.getDbzConfiguration()),
                        new JdbcConnectionFactory(jdbcSourceConfig, connectionPoolFactory),
                        null,
                        null)) {
            TableId tableId =
                    new TableId(null, getSchemaName(conn, databaseName, tableName), tableName);
            Tables tables = new Tables();
            conn.readSchema(
                    tables,
                    null,
                    tableId.schema(),
                    t -> t.table().equalsIgnoreCase(tableName),
                    null,
                    false);
            List<String> columnNames = new ArrayList<>();
            List<DataType> columnTypes = new ArrayList<>();
            tables.forTable(tableId)
                    .columns()
                    .forEach(
                            column -> {
                                String columnName = column.name();
                                DataType flinkType = convertColumnType(column);
                                columnNames.add(columnName);
                                columnTypes.add(flinkType);
                            });
            return Schema.newBuilder().fromFields(columnNames, columnTypes).build();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataType convertColumnType(Column column) {
        if (column.name().equalsIgnoreCase("FORGID")) {
            return OracleTypeUtils.fromDbzColumn(column);
        }
        return OracleTypeUtils.fromDbzColumn(column);
    }
}
