/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.relational;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.SchemaHistoryMetrics;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Copied from Debezium project. Configuration options shared across the relational CDC connectors
 * which use a persistent database schema history.
 *
 * <p>Added JMX_METRICS_ENABLED option.
 */
public abstract class HistorizedRelationalDatabaseConnectorConfig
        extends RelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;

    private boolean useCatalogBeforeSchema;
    private final Class<? extends SourceConnector> connectorClass;
    private final boolean multiPartitionMode;

    /**
     * The database history class is hidden in the {@link #configDef()} since that is designed to
     * work with a user interface, and in these situations using Kafka is the only way to go.
     */
    public static final Field DATABASE_HISTORY =
            Field.create("database.history")
                    .withDisplayName("Database history class")
                    .withType(Type.CLASS)
                    .withWidth(Width.LONG)
                    .withImportance(Importance.LOW)
                    .withInvisibleRecommender()
                    .withDescription(
                            "The name of the DatabaseHistory class that should be used to store and recover database schema changes. "
                                    + "The configuration properties for the history are prefixed with the '"
                                    + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                                    + "' string.")
                    .withDefault(KafkaSchemaHistory.class.getName());

    public static final Field JMX_METRICS_ENABLED =
            Field.create(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "metrics.enabled")
                    .withDisplayName("Skip DDL statements that cannot be parsed")
                    .withType(Type.BOOLEAN)
                    .withImportance(Importance.LOW)
                    .withDescription("Whether to enable JMX history metrics")
                    .withDefault(false);

    protected static final ConfigDefinition CONFIG_DEFINITION =
            RelationalDatabaseConnectorConfig.CONFIG_DEFINITION
                    .edit()
                    .history(
                            DATABASE_HISTORY,
                            SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS,
                            SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL,
                            KafkaSchemaHistory.BOOTSTRAP_SERVERS,
                            KafkaSchemaHistory.TOPIC,
                            KafkaSchemaHistory.RECOVERY_POLL_ATTEMPTS,
                            KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS,
                            KafkaSchemaHistory.KAFKA_QUERY_TIMEOUT_MS)
                    .create();

    protected HistorizedRelationalDatabaseConnectorConfig(
            Class<? extends SourceConnector> connectorClass,
            Configuration config,
            TableFilter systemTablesFilter,
            boolean useCatalogBeforeSchema,
            int defaultSnapshotFetchSize,
            ColumnFilterMode columnFilterMode,
            boolean multiPartitionMode) {
        super(
                config,
                systemTablesFilter,
                TableId::toString,
                defaultSnapshotFetchSize,
                columnFilterMode,
                useCatalogBeforeSchema);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.connectorClass = connectorClass;
        this.multiPartitionMode = multiPartitionMode;
    }

    protected HistorizedRelationalDatabaseConnectorConfig(
            Class<? extends SourceConnector> connectorClass,
            Configuration config,
            TableFilter systemTablesFilter,
            TableIdToStringMapper tableIdMapper,
            boolean useCatalogBeforeSchema,
            ColumnFilterMode columnFilterMode,
            boolean multiPartitionMode) {
        super(
                config,
                systemTablesFilter,
                tableIdMapper,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                columnFilterMode,
                useCatalogBeforeSchema);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.connectorClass = connectorClass;
        this.multiPartitionMode = multiPartitionMode;
    }

    /** Returns a configured (but not yet started) instance of the database history. */
    public SchemaHistory getDatabaseHistory() {
        Configuration config = getConfig();

        SchemaHistory databaseHistory =
                config.getInstance(
                        HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY,
                        SchemaHistory.class);
        if (databaseHistory == null) {
            throw new ConnectException(
                    "Unable to instantiate the database history class "
                            + config.getString(
                                    HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY));
        }

        // Do not remove the prefix from the subset of config properties ...
        Configuration dbHistoryConfig =
                config.subset(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                        .edit()
                        .withDefault(SchemaHistory.NAME, getLogicalName() + "-dbhistory")
                        .withDefault(
                                SchemaHistory.INTERNAL_CONNECTOR_CLASS, connectorClass.getName())
                        .withDefault(KafkaSchemaHistory.INTERNAL_CONNECTOR_ID, logicalName)
                        .build();

        SchemaHistoryListener listener =
                config.getBoolean(JMX_METRICS_ENABLED)
                        ? new SchemaHistoryMetrics(this, multiPartitionMode)
                        : SchemaHistoryListener.NOOP;

        HistoryRecordComparator historyComparator = getHistoryRecordComparator();
        databaseHistory.configure(
                dbHistoryConfig, historyComparator, listener, useCatalogBeforeSchema); // validates

        return databaseHistory;
    }

    public boolean useCatalogBeforeSchema() {
        return useCatalogBeforeSchema;
    }

    /**
     * Returns a comparator to be used when recovering records from the schema history, making sure
     * no history entries newer than the offset we resume from are recovered (which could happen
     * when restarting a connector after history records have been persisted but no new offset has
     * been committed yet).
     */
    protected abstract HistoryRecordComparator getHistoryRecordComparator();
}
