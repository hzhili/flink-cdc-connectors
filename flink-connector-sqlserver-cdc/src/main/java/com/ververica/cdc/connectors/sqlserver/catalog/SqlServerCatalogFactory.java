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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerJdbcConfiguration;

import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.base.catalog.JdbcCatalogOptions.ENABLE_METADATA_COLUMN;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory.DATABASE_NAME;
import static com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory.HOSTNAME;
import static com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory.PASSWORD;
import static com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory.PORT;
import static com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory.SERVER_TIME_ZONE;
import static com.ververica.cdc.connectors.sqlserver.table.SqlServerTableFactory.USERNAME;
import static com.ververica.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;

/** Sql Server catalog factory. */
public class SqlServerCatalogFactory implements CatalogFactory {
    private static final String IDENTIFIER = "sqlserver-ctl";

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(DEBEZIUM_OPTIONS_PREFIX);
        Configuration configuration = Configuration.from(context.getOptions());
        SqlServerJdbcConfiguration.Builder jdbcConfigBuilder =
                SqlServerJdbcConfiguration.copy(
                        configuration.subset(DEBEZIUM_OPTIONS_PREFIX, true));
        String hostname = configuration.getString(HOSTNAME.key());
        jdbcConfigBuilder
                .with(SqlServerConnectorConfig.HOSTNAME, hostname)
                .with(
                        SqlServerConnectorConfig.PORT,
                        configuration.getInteger(PORT.key(), PORT.defaultValue()));
        configuration
                .edit()
                .with(PORT.key(), configuration.getInteger(PORT.key(), PORT.defaultValue()));
        String databaseName = configuration.getString(DATABASE_NAME.key()).toUpperCase();
        configuration.edit().with(DATABASE_NAME.key(), databaseName);
        SqlServerJdbcConfiguration jdbcConfiguration =
                jdbcConfigBuilder
                        .with(SqlServerConnectorConfig.DATABASE_NAME, databaseName)
                        .with(
                                SqlServerConnectorConfig.USER,
                                configuration.getString(USERNAME.key()))
                        .with(
                                SqlServerConnectorConfig.PASSWORD,
                                configuration.getString(PASSWORD.key()))
                        .build();
        return new SqlServerCatalog(
                context.getName(), databaseName, configuration, jdbcConfiguration);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SERVER_TIME_ZONE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(ENABLE_METADATA_COLUMN);
        return options;
    }
}
