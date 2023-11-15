/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.sqlserver.source.reader.fetch;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import com.ververica.cdc.connectors.base.utils.SourceRecordUtils;
import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import com.ververica.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import com.ververica.cdc.connectors.sqlserver.source.offset.LsnOffset;
import com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerUtils;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.ChangeEventQueue.Builder;
import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerErrorHandler;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerOffsetContext.Loader;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.connector.sqlserver.SqlServerTaskContext;
import io.debezium.data.Envelope.FieldName;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/** The context for fetch task that fetching data of snapshot split from SqlServer data source. */
public class SqlServerSourceFetchTaskContext extends JdbcSourceFetchTaskContext {
    private static final Logger LOG =
            LoggerFactory.getLogger(SqlServerSourceFetchTaskContext.class);

    /** Connection used for reading CDC tables. */
    private final SqlServerConnection connection;

    /**
     * A separate connection for retrieving details of the schema changes; without it, adaptive
     * buffering will not work.
     *
     * <p>For more details, please refer to <a
     * href="https://docs.microsoft.com/en-us/sql/connect/jdbc/using-adaptive-buffering?view=sql-server-2017#guidelines-for-using-adaptive-buffering">guidelines-for-using-adaptive-buffering</a>
     */
    private final SqlServerConnection metaDataConnection;

    private final SqlServerEventMetadataProvider metadataProvider;
    private SqlServerOffsetContext offsetContext;
    private SqlServerPartition partition;
    private SqlServerDatabaseSchema databaseSchema;
    private JdbcSourceEventDispatcher<SqlServerPartition> dispatcher;
    private SqlServerErrorHandler errorHandler;
    private ChangeEventQueue<DataChangeEvent> queue;
    private SqlServerTaskContext taskContext;
    private TopicNamingStrategy<TableId> topicNamingStrategy;
    private EventDispatcher.SnapshotReceiver<SqlServerPartition> snapshotReceiver;
    private SnapshotChangeEventSourceMetrics<SqlServerPartition> snapshotChangeEventSourceMetrics;
    private StreamingChangeEventSourceMetrics<SqlServerPartition> streamingChangeEventSourceMetrics;

    public SqlServerSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            SqlServerDialect dataSourceDialect,
            SqlServerConnection connection,
            SqlServerConnection metaDataConnection) {
        super(sourceConfig, dataSourceDialect);
        this.connection = connection;
        this.metadataProvider = new SqlServerEventMetadataProvider();
        this.metaDataConnection = metaDataConnection;
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        // initial stateful objects
        final SqlServerConnectorConfig connectorConfig = getDbzConnectorConfig();
        this.topicNamingStrategy =
                connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        EmbeddedFlinkDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedFlinkDatabaseHistory.SCHEMA_HISTORY_INTERNAL_NAME),
                sourceSplitBase.getTableSchemas().values());

        this.databaseSchema =
                SqlServerUtils.createSqlServerDatabaseSchema(connectorConfig, connection);
        this.offsetContext = loadStartingOffsetState(new Loader(connectorConfig), sourceSplitBase);

        String serverName = connectorConfig.getLogicalName();
        String dbName = connectorConfig.getJdbcConfig().getDatabase();
        this.partition = new SqlServerPartition(serverName, dbName);

        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new SqlServerTaskContext(connectorConfig, databaseSchema);

        final int queueSize = getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
        this.queue =
                new Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "sqlserver-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        //                         .buffering()
                        .build();
        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicNamingStrategy,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster);

        this.snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();

        final DefaultChangeEventSourceMetricsFactory<SqlServerPartition>
                changeEventSourceMetricsFactory = new DefaultChangeEventSourceMetricsFactory<>();
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getStreamingMetrics(
                        taskContext, queue, metadataProvider);
        this.errorHandler = new SqlServerErrorHandler(connectorConfig, queue);
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private SqlServerOffsetContext loadStartingOffsetState(
            SqlServerOffsetContext.Loader loader, SourceSplitBase sourceSplitBase) {
        Offset offset =
                sourceSplitBase.isSnapshotSplit()
                        ? LsnOffset.INITIAL_OFFSET
                        : sourceSplitBase.asStreamSplit().getStartingOffset();

        return loader.load(offset.getOffset());
    }

    private void validateAndLoadDatabaseHistory(
            SqlServerOffsetContext offset, SqlServerDatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(partition, offset);
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return SqlServerUtils.getLsn(record);
    }

    @Override
    public SqlServerDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public RowType getSplitType(Table table) {
        return SqlServerUtils.getSplitType(table);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public SqlServerConnectorConfig getDbzConnectorConfig() {
        return (SqlServerConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public SqlServerSourceConfig getSourceConfig() {
        return (SqlServerSourceConfig) sourceConfig;
    }

    @Override
    public JdbcSourceEventDispatcher<SqlServerPartition> getDispatcher() {
        return dispatcher;
    }

    public EventDispatcher.SnapshotReceiver<SqlServerPartition> getSnapshotReceiver() {
        return snapshotReceiver;
    }

    @Override
    public SqlServerOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public SqlServerPartition getPartition() {
        return partition;
    }

    public SqlServerConnection getConnection() {
        return connection;
    }

    public SqlServerConnection getMetaDataConnection() {
        return metaDataConnection;
    }

    public SnapshotChangeEventSourceMetrics<SqlServerPartition>
            getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public StreamingChangeEventSourceMetrics<SqlServerPartition>
            getStreamingChangeEventSourceMetrics() {
        return streamingChangeEventSourceMetrics;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        return SourceRecordUtils.getTableId(record);
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        return SourceRecordUtils.isDataChangeRecord(record);
    }

    @Override
    public void close() throws Exception {
        metaDataConnection.close();
        connection.close();
    }

    @Override
    public SchemaNameAdjuster getSchemaNameAdjuster() {
        return schemaNameAdjuster;
    }

    /** Copied from debezium for accessing here. */
    public static class SqlServerEventMetadataProvider implements EventMetadataProvider {

        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
            return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return Collect.hashMapOf(
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY),
                    SourceInfo.CHANGE_LSN_KEY, sourceInfo.getString(SourceInfo.CHANGE_LSN_KEY));
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY);
        }
    }
}
