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

package com.ververica.cdc.connectors.base.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Abstract jdbc catalog. */
public abstract class AbstractJdbcCatalog extends AbstractCatalog {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJdbcCatalog.class);
    protected final JdbcSourceConfig jdbcSourceConfig;
    protected final JdbcConnectionPoolFactory connectionPoolFactory;
    protected final JdbcConfiguration jdbcConfiguration;
    protected final JdbcConnectionFactory jdbcConnectionFactory;

    public AbstractJdbcCatalog(
            String name,
            String defaultDatabase,
            JdbcSourceConfig jdbcSourceConfig,
            JdbcConnectionPoolFactory connectionPoolFactory) {
        super(name, defaultDatabase);
        this.jdbcSourceConfig = jdbcSourceConfig;
        this.connectionPoolFactory = connectionPoolFactory;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcSourceConfig.getDbzConfiguration());
        this.jdbcConnectionFactory =
                new JdbcConnectionFactory(jdbcSourceConfig, connectionPoolFactory);
    }

    @Override
    public void open() throws CatalogException {
        try (JdbcConnection connection =
                new JdbcConnection(jdbcConfiguration, jdbcConnectionFactory, null, null)) {
            LOGGER.info(
                    "Catalog {} established connection to {}",
                    getName(),
                    connectionPoolFactory.getJdbcUrl(jdbcSourceConfig));
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Failed testing connection for %s with user '%s'",
                            jdbcConfiguration.withMaskedPasswords(),
                            jdbcSourceConfig.getUsername()),
                    e);
        }
    }

    @Override
    public void close() throws CatalogException {
        try {
            LOGGER.info("Closed catalog {} ", getName());
        } catch (Exception e) {
            throw new CatalogException(String.format("Closing catalog %s failed.", getName()), e);
        }
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(getDynamicTableFactory());
    }

    public abstract DynamicTableFactory getDynamicTableFactory();

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (JdbcConnection connection =
                new JdbcConnection(jdbcConfiguration, jdbcConnectionFactory, null, null)) {
            return new ArrayList<>(connection.readAllCatalogNames());
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Catalog %s failed get database.", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
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
                            databaseName,
                            getSchemaName(conn, databaseName, null),
                            null,
                            new String[] {"TABLE"});
            return tableIds.stream().map(TableId::table).collect(Collectors.toList());
        } catch (SQLException e) {
            throw new CatalogException(e);
        }
    }

    public abstract String getSchemaName(
            JdbcConnection conn, String databaseName, String tableName);

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getObjectName();
        Map<String, String> options = getTableOptions(tablePath);

        return CatalogTable.of(
                createTableSchema(databaseName, tableName), null, Lists.newArrayList(), options);
    }

    public abstract Map<String, String> getTableOptions(ObjectPath tablePath);

    public Schema createTableSchema(String databaseName, String tableName) {
        try (JdbcConnection conn =
                new JdbcConnection(
                        JdbcConfiguration.adapt(jdbcSourceConfig.getDbzConfiguration()),
                        new JdbcConnectionFactory(jdbcSourceConfig, connectionPoolFactory),
                        null,
                        null)) {
            TableId tableId =
                    new TableId(
                            databaseName, getSchemaName(conn, databaseName, tableName), tableName);
            Tables tables = new Tables();
            conn.readSchema(
                    tables,
                    databaseName,
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

    public abstract DataType convertColumnType(Column column);

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
