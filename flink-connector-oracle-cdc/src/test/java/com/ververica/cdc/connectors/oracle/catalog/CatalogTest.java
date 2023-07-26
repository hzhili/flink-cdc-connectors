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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.debezium.config.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.USERNAME;

/** Oracle catalog test. */
@Ignore
public class CatalogTest {
    private StreamTableEnvironment tEnv;
    private OracleCatalog catalog;

    @Before
    public void setup() {
        Map<String, String> props = new HashMap<>();
        props.put(HOSTNAME.key(), "172.16.89.33");
        props.put(USERNAME.key(), "cdcuser");
        props.put(PASSWORD.key(), "cdcuser");
        props.put(DATABASE_NAME.key(), "HISDB");
        props.put(SCHEMA_NAME.key(), "BSHIS60");
        props.put("scan.startup.mode", "initial");
        catalog = new OracleCatalog("oracle", "BSHIS60", Configuration.from(props));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //        env.setParallelism(1);
        this.tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.registerCatalog("oracle", catalog);
        tEnv.useCatalog("oracle");
    }

    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        actual.forEach(System.out::println);
    }

    @Test
    public void testListTables() {
        tEnv.executeSql("use BSHIS60");
        tEnv.executeSql("show tables").print();
    }

    @Test
    public void testQuery() {
        tEnv.executeSql("use BSHIS60");
        tEnv.executeSql(
                        "SELECT * FROM MZSF_CLININFO /*+OPTIONS('scan.startup.mode'='latest-offset')*/")
                .print();
    }
}
