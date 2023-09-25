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

package com.ververica.cdc.connectors.oracle.source.reader.fetch;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerOracleOffsetContextLoader;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

import static com.ververica.cdc.connectors.oracle.source.reader.fetch.OracleStreamFetchTask.RedoLogSplitReadTask;
import static com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils.currentRedoLogOffset;
import static com.ververica.cdc.connectors.oracle.source.utils.OracleUtils.buildSplitScanQuery;
import static com.ververica.cdc.connectors.oracle.source.utils.OracleUtils.readTableSplitDataStatement;

/** The task to work for fetching data of Oracle table snapshot split. */
public class OracleScanFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    private OracleSnapshotSplitReadTask snapshotSplitReadTask;

    public OracleScanFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public SnapshotSplit getSplit() {
        return split;
    }

    @Override
    public void close() {
        taskRunning = false;
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void execute(Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
                new OracleSnapshotSplitReadTask(
                        sourceFetchContext.getSourceConfig(),
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        split);
        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SnapshotSplitChangeEventSourceContext();
        SnapshotResult<OracleOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());

        final StreamSplit backfillBinlogSplit =
                createBackfillRedoLogSplit(changeEventSourceContext);
        // optimization that skip the binlog read when the low watermark equals high
        // watermark
        final boolean binlogBackfillRequired =
                backfillBinlogSplit
                        .getEndingOffset()
                        .isAfter(backfillBinlogSplit.getStartingOffset());
        if (!binlogBackfillRequired) {
            dispatchBinlogEndEvent(
                    backfillBinlogSplit,
                    sourceFetchContext.getPartition().getSourcePartition(),
                    ((OracleSourceFetchTaskContext) context).getDispatcher());
            taskRunning = false;
            return;
        }
        // execute redoLog read task
        if (snapshotResult.isCompletedOrSkipped()) {
            final RedoLogSplitReadTask backfillBinlogReadTask =
                    createBackfillRedoLogReadTask(backfillBinlogSplit, sourceFetchContext);

            final LogMinerOracleOffsetContextLoader loader =
                    new LogMinerOracleOffsetContextLoader(
                            ((OracleSourceFetchTaskContext) context).getDbzConnectorConfig());
            final OracleOffsetContext oracleOffsetContext =
                    loader.load(backfillBinlogSplit.getStartingOffset().getOffset());
            backfillBinlogReadTask.execute(
                    new SnapshotBinlogSplitChangeEventSourceContext(),
                    sourceFetchContext.getPartition(),
                    oracleOffsetContext);
            taskRunning = false;
        } else {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for oracle split %s fail", split));
        }
    }

    private StreamSplit createBackfillRedoLogSplit(
            SnapshotSplitChangeEventSourceContext sourceContext) {
        return new StreamSplit(
                split.splitId(),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>(),
                split.getTableSchemas(),
                0);
    }

    private RedoLogSplitReadTask createBackfillRedoLogReadTask(
            StreamSplit backfillBinlogSplit, OracleSourceFetchTaskContext context) {
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
                context.getSourceConfig()
                        .getDbzConfiguration()
                        .edit()
                        .with("table.include.list", split.getTableId().toString())
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();
        // task to read binlog and backfill for current split
        return new RedoLogSplitReadTask(
                new OracleConnectorConfig(dezConf),
                context.getConnection(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                context.getSourceConfig().getOriginDbzConnectorConfig(),
                context.getStreamingChangeEventSourceMetrics(),
                backfillBinlogSplit);
    }

    private void dispatchBinlogEndEvent(
            StreamSplit backFillBinlogSplit,
            Map<String, ?> sourcePartition,
            JdbcSourceEventDispatcher<OraclePartition> eventDispatcher)
            throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
                sourcePartition,
                backFillBinlogSplit,
                backFillBinlogSplit.getEndingOffset(),
                WatermarkKind.END);
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class OracleSnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<OraclePartition, OracleOffsetContext> {

        private static final Logger LOG =
                LoggerFactory.getLogger(OracleSnapshotSplitReadTask.class);

        /** Interval for showing a log statement with the progress while scanning a single table. */
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

        private final OracleConnectorConfig connectorConfig;
        private final OracleDatabaseSchema databaseSchema;
        private final OracleConnection jdbcConnection;
        private final JdbcSourceEventDispatcher<OraclePartition> dispatcher;
        private final Clock clock;
        private final SnapshotSplit snapshotSplit;
        private final OracleOffsetContext offsetContext;
        private final SnapshotProgressListener<OraclePartition> snapshotProgressListener;
        private final OracleSourceConfig sourceConfig;

        public OracleSnapshotSplitReadTask(
                OracleSourceConfig sourceConfig,
                OracleConnectorConfig connectorConfig,
                OracleOffsetContext previousOffset,
                SnapshotProgressListener<OraclePartition> snapshotProgressListener,
                OracleDatabaseSchema databaseSchema,
                OracleConnection jdbcConnection,
                JdbcSourceEventDispatcher<OraclePartition> dispatcher,
                SnapshotSplit snapshotSplit) {
            super(connectorConfig, snapshotProgressListener);
            this.sourceConfig = sourceConfig;
            this.offsetContext = previousOffset;
            this.connectorConfig = connectorConfig;
            this.databaseSchema = databaseSchema;
            this.jdbcConnection = jdbcConnection;
            this.dispatcher = dispatcher;
            this.clock = Clock.SYSTEM;
            this.snapshotSplit = snapshotSplit;
            this.snapshotProgressListener = snapshotProgressListener;
        }

        @Override
        public SnapshotResult<OracleOffsetContext> execute(
                ChangeEventSourceContext context,
                OraclePartition partition,
                OracleOffsetContext previousOffset)
                throws InterruptedException {
            SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
            final SnapshotContext<OraclePartition, OracleOffsetContext> ctx;
            try {
                ctx = prepare(partition);
            } catch (Exception e) {
                LOG.error("Failed to initialize snapshot context.", e);
                throw new RuntimeException(e);
            }
            try {
                return doExecute(context, previousOffset, ctx, snapshottingTask);
            } catch (InterruptedException e) {
                LOG.warn("Snapshot was interrupted before completion");
                throw e;
            } catch (Exception t) {
                throw new DebeziumException(t);
            }
        }

        @Override
        protected SnapshotResult<OracleOffsetContext> doExecute(
                ChangeEventSourceContext context,
                OracleOffsetContext previousOffset,
                SnapshotContext snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final OracleSnapshotContext ctx = (OracleSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;

            final RedoLogOffset lowWatermark = currentRedoLogOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 1 - Determining low watermark {} for split {}",
                    lowWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) (context)).setLowWatermark(lowWatermark);
            dispatcher.dispatchWatermarkEvent(
                    snapshotContext.partition.getSourcePartition(),
                    snapshotSplit,
                    lowWatermark,
                    WatermarkKind.LOW);

            LOG.info("Snapshot step 2 - Snapshotting data");
            createDataEvents(ctx, snapshotSplit.getTableId());

            final RedoLogOffset highWatermark = currentRedoLogOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 3 - Determining high watermark {} for split {}",
                    highWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) (context)).setHighWatermark(highWatermark);
            dispatcher.dispatchWatermarkEvent(
                    snapshotContext.partition.getSourcePartition(),
                    snapshotSplit,
                    highWatermark,
                    WatermarkKind.HIGH);
            return SnapshotResult.completed(ctx.offset);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                OraclePartition partition, OracleOffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected SnapshotContext<OraclePartition, OracleOffsetContext> prepare(
                OraclePartition partition) throws Exception {
            return new OracleSnapshotContext(partition);
        }

        private static class OracleSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        OraclePartition, OracleOffsetContext> {

            public OracleSnapshotContext(OraclePartition partition) throws SQLException {
                super(partition, "");
            }
        }

        private void createDataEvents(OracleSnapshotContext snapshotContext, TableId tableId)
                throws Exception {
            EventDispatcher.SnapshotReceiver<OraclePartition> snapshotReceiver =
                    dispatcher.getSnapshotChangeEventReceiver();
            LOG.debug("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                OracleSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<OraclePartition> snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());

            final String selectSql =
                    buildSplitScanQuery(
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null);
            LOG.info(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    table.id(),
                    selectSql);

            try (PreparedStatement selectStatement =
                            readTableSplitDataStatement(
                                    jdbcConnection,
                                    selectSql,
                                    snapshotSplit.getSplitStart() == null,
                                    snapshotSplit.getSplitEnd() == null,
                                    snapshotSplit.getSplitStart(),
                                    snapshotSplit.getSplitEnd(),
                                    snapshotSplit.getSplitKeyType().getFieldCount(),
                                    sourceConfig.getFetchSize());
                    ResultSet rs = selectStatement.executeQuery()) {

                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    final Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        snapshotProgressListener.rowsScanned(
                                snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    dispatcher.dispatchSnapshotEvent(
                            snapshotContext.partition,
                            table.id(),
                            getChangeRecordEmitter(snapshotContext, table.id(), row),
                            snapshotReceiver);
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
            }
        }

        protected ChangeRecordEmitter<OraclePartition> getChangeRecordEmitter(
                SnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                TableId tableId,
                Object[] row) {
            snapshotContext.offset.event(tableId, clock.currentTime());
            return new SnapshotChangeRecordEmitter<>(
                    snapshotContext.partition, snapshotContext.offset, row, clock);
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link SnapshotSplit}.
     */
    public static class SnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        private RedoLogOffset lowWatermark;
        private RedoLogOffset highWatermark;

        public RedoLogOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(RedoLogOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public RedoLogOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(RedoLogOffset highWatermark) {
            this.highWatermark = highWatermark;
        }

        @Override
        public boolean isRunning() {
            return lowWatermark != null && highWatermark != null;
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded binlog task
     * of a snapshot split task.
     */
    public class SnapshotBinlogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            taskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
