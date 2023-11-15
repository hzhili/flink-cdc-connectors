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

package com.ververica.cdc.connectors.oracle.source.meta.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.oracle.source.OracleDialect;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import io.debezium.jdbc.JdbcConnection;

import java.sql.Timestamp;
import java.util.Map;

/** An offset factory class create {@link RedoLogOffset} instance. */
public class RedoLogOffsetFactory extends OffsetFactory {

    private static final long serialVersionUID = 1L;

    private final OracleDialect dialect;
    private final OracleSourceConfig sourceConfig;
    private static final String TIMESTAMP_TO_LSN = "SELECT timestamp_to_scn(?) from dual";

    public RedoLogOffsetFactory(OracleSourceConfigFactory configFactory, OracleDialect dialect) {
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    @Override
    public Offset newOffset(Map<String, String> offset) {
        return new RedoLogOffset(offset);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        throw new UnsupportedOperationException(
                "Do not support to create RedoLogOffset by filename and position.");
    }

    @Override
    public Offset newOffset(Long position) {
        throw new UnsupportedOperationException(
                "Do not support to create RedoLogOffset by position.");
    }

    @Override
    public Offset createTimestampOffset(long timestampMillis) {
        Timestamp timestamp = new Timestamp(timestampMillis);
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return jdbcConnection.prepareQueryAndMap(
                    TIMESTAMP_TO_LSN,
                    ps -> ps.setTimestamp(1, timestamp),
                    rs -> {
                        if (rs.next()) {
                            return new RedoLogOffset(rs.getLong(1));
                        }
                        return null;
                    });
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset createInitialOffset() {
        return RedoLogOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset createNoStoppingOffset() {
        return RedoLogOffset.NO_STOPPING_OFFSET;
    }
}
