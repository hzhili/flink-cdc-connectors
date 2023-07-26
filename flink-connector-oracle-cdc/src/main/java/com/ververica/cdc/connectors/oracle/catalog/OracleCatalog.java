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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.TABLE_NAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Oracle catalog implements,extends {@link AbstractJdbcCatalog }. */
public class OracleCatalog extends AbstractJdbcCatalog {
    private static final List<String> ORACLE_SYSTEM_USER =
            Arrays.asList("SYS", "SYSTEM", "SYSAUX", "SYSMAN", "DBSNMP", "OUTLN", "APPQOSSYS");
    private static final int FETCH_SIZE = 1024;
    private final Configuration configuration;
    private final JdbcConfiguration jdbcConfiguration;

    public OracleCatalog(
            String name,
            String defaultDatabase,
            Configuration configuration,
            JdbcConfiguration jdbcConfiguration) {
        super(name, defaultDatabase);
        this.configuration = configuration;
        this.jdbcConfiguration = jdbcConfiguration;
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
        options.put(CONNECTOR.key(), "oracle-cdc");
        options.put(TABLE_NAME.key(), tablePath.getObjectName());
        return options;
    }

    @Override
    public Schema createTableSchema(String databaseName, String tableName) throws SQLException {
        Table table =
                ((OracleConnection) connection)
                        .readTableSchema(
                                jdbcConfiguration.getDatabase(), databaseName, tableName, null)
                        .forTable(
                                new TableId(
                                        jdbcConfiguration.getDatabase(), databaseName, tableName));
        List<Column> columns = table.columns();
        List<String> fieldNames = new ArrayList<>(columns.size());
        List<DataType> fieldTypes = new ArrayList<>(columns.size());
        columns.forEach(
                column -> {
                    fieldNames.add(column.name());
                    fieldTypes.add(convertColumnType(column));
                });
        Schema.Builder builder = Schema.newBuilder().fromFields(fieldNames, fieldTypes);
        builder.primaryKey(table.primaryKeyColumnNames()).withComment(table.comment());
        return builder.build();
    }

    @Override
    public DataType convertColumnType(Column column) {
        return OracleTypeUtils.fromDbzColumn(column);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        String query =
                "SELECT USERNAME FROM DBA_USERS WHERE ACCOUNT_STATUS='OPEN' "
                        + "AND USERNAME NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'SYSMAN', 'DBSNMP', 'OUTLN', 'APPQOSSYS')";
        try {
            return connection.queryAndMap(
                    query,
                    rs -> {
                        List<String> databases = new ArrayList<>();
                        rs.setFetchSize(FETCH_SIZE);
                        while (rs.next()) {
                            databases.add(rs.getString("USERNAME"));
                        }
                        return databases.parallelStream().sorted().collect(Collectors.toList());
                    });
            // return new ArrayList<>(connection.readAllSchemaNames(schema ->
            // !ORACLE_SYSTEM_USER.contains(schema)));
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
                        ps.setString(1, databaseName);
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
