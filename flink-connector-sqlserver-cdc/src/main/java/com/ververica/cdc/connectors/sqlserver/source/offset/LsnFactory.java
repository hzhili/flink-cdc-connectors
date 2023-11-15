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

package com.ververica.cdc.connectors.sqlserver.source.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import com.ververica.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.jdbc.JdbcConnection;

import java.sql.Timestamp;
import java.util.Map;

/** A factory to create {@link LsnOffset}. */
public class LsnFactory extends OffsetFactory {
    private final SqlServerSourceConfig sourceConfig;

    private final SqlServerDialect dialect;

    private static final String TIMESTAMP_TO_LSN =
            "SELECT sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',?)";

    public LsnFactory(SqlServerSourceConfigFactory configFactory, SqlServerDialect dialect) {
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    @Override
    public Offset newOffset(Map<String, String> offset) {
        Lsn changeLsn = Lsn.valueOf(offset.get(SourceInfo.CHANGE_LSN_KEY));
        Lsn commitLsn = Lsn.valueOf(offset.get(SourceInfo.COMMIT_LSN_KEY));
        return new LsnOffset(changeLsn, commitLsn, null);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset newOffset(Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset createTimestampOffset(long timestampMillis) {
        Timestamp timestamp =
                new Timestamp(timestampMillis);
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return jdbcConnection.prepareQueryAndMap(
                    TIMESTAMP_TO_LSN,
                    ps -> ps.setTimestamp(1, timestamp),
                    rs -> {
                        if (rs.next()) {
                            return new LsnOffset(null, Lsn.valueOf(rs.getBytes(1)), null);
                        }
                        return null;
                    });
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset createInitialOffset() {
        return LsnOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset createNoStoppingOffset() {
        return LsnOffset.NO_STOPPING_OFFSET;
    }
}
