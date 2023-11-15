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

package com.ververica.cdc.connectors.sqlserver.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Before;
import org.junit.Test;

/** Integration tests for SqlServer Table source. */
public class DebugTable {
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @Before
    public void before() {
        //        env.enableCheckpointing(200);
        env.setParallelism(1);
    }

    @Test
    public void debugTest() throws Throwable {
        String sourceDDL =
                "create catalog sqlCatalog with(\n"
                        + "  'type'='sqlserver-ctl',\n"
                        + "  'hostname'='192.168.0.2',\n"
                        + "  'port'='1433',\n"
                        + "  'username'='sa',\n"
                        + "  'password'='146-164-156-',\n"
                        + "  'database-name'='his_new',\n"
                        + "  'schema-name'='dbo',\n"
                        + "  'enable.metadata.column'='true',\n"
                        + "  'debezium.database.encrypt'='false'\n"
                        + ");";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(
                        "SELECT * FROM sqlCatalog.his_new.DictMedi /*+OPTIONS('scan.incremental.close-idle-reader.enabled'='true',\n"
                                + "'scan.incremental.snapshot.chunk.size'='1024')*/ A")
                .print();
    }
}
