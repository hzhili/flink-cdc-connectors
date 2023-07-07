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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.options.JdbcSourceOptions;
import com.ververica.cdc.connectors.base.options.SourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.source.OraclePooledDataSourceFactory;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.util.OptionValidateUtil;
import io.debezium.config.Configuration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.USERNAME;
import static com.ververica.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions.PORT;
import static com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions.URL;
import static com.ververica.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Oracle catalog factory. */
public class OracleCatalogFactory implements CatalogFactory {
    private static final String DRIVER_ClASS_NAME = "oracle.jdbc.OracleDriver";

    @Override
    public Catalog createCatalog(Context context) {
        Configuration configuration = Configuration.from(context.getOptions());
        String databaseName = configuration.getString(DATABASE_NAME.key()).toUpperCase();
        configuration.edit().with(DATABASE_NAME.key(), databaseName);
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(DEBEZIUM_OPTIONS_PREFIX);
        validate(helper.getOptions());
        String defaultDatabase = configuration.getString(SCHEMA_NAME.key());
        return new OracleCatalog(
                context.getName(),
                defaultDatabase,
                createSourceConfig(configuration),
                new OraclePooledDataSourceFactory(),
                configuration.asMap());
    }

    private void validate(ReadableConfig config) {
        if (config.get(URL) == null) {
            checkNotNull(config.get(HOSTNAME), "hostname is required when url is not configured");
        }
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        double distributionFactorUpper = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        OptionValidateUtil.validateIntegerOption(
                SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        OptionValidateUtil.validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        OptionValidateUtil.validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        OptionValidateUtil.validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        OptionValidateUtil.validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        OptionValidateUtil.validateDistributionFactorUpper(distributionFactorUpper);
        OptionValidateUtil.validateDistributionFactorLower(distributionFactorLower);
    }

    public JdbcSourceConfig createSourceConfig(Configuration configuration) {
        return new OracleSourceConfig(
                StartupOptions.initial(),
                Collections.singletonList(configuration.getString(DATABASE_NAME.key())),
                null,
                SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                false,
                true,
                configuration.asProperties(),
                configuration,
                DRIVER_ClASS_NAME,
                null,
                configuration.getString(HOSTNAME.key()),
                configuration.getInteger(PORT.key(), PORT.defaultValue()),
                configuration.getString(USERNAME.key()),
                configuration.getString(PASSWORD.key()),
                SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                JdbcSourceOptions.SERVER_TIME_ZONE.defaultValue(),
                JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                null);
    }

    @Override
    public String factoryIdentifier() {
        return "oracle-ctl";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(URL);
        options.add(HOSTNAME);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        return options;
    }
    /** Checks the value of given integer option is valid. */
}
