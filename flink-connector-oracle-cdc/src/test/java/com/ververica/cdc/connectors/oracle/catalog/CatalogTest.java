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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Oracle catalog test. */
@Ignore
public class CatalogTest {
    private StreamTableEnvironment tEnv;
    private OracleCatalog catalog;

    @Before
    public void setup() {
        //        Map<String, String> props = new HashMap<>();
        //        props.put(HOSTNAME.key(), "192.168.0.240");
        //        props.put(USERNAME.key(), "cdc_admin");
        //        props.put(PASSWORD.key(), "Xyh@3613571@cdc");
        //        props.put(DATABASE_NAME.key(), "orcl");
        //        props.put(SCHEMA_NAME.key(), "BSHIS_JXFY");
        //        catalog = new OracleCatalog("oracle", "BSHIS60", Configuration.from(props),null);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        //        env.setParallelism(1);
        this.tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        //        tEnv.registerCatalog("oracle", catalog);
        //        tEnv.useCatalog("oracle");
        tEnv.executeSql(
                "create catalog oraCatalog with(\n"
                        + "  'type'='oracle-ctl',\n"
                        + "  'hostname'='192.168.0.32',\n"
                        + "  'port'='1521',\n"
                        + "  'username'='CDC_ADMIN',\n"
                        + "  'password'='Xyh@3613571@cdc',\n"
                        + "  'database-name'='ORCL',\n"
                        + "  'schema-name'='BSHIS2020_EY',\n"
                        + "  'enable.metadata.column'='true'\n"
                        + ");");
        //        tEnv.executeSql("use catalog oraCatalog");
    }

    @Test
    public void testListDatabases() {
        //        List<String> actual = catalog.listDatabases();
        //        actual.forEach(System.out::println);
        tEnv.executeSql("show databases;").print();
    }

    @Test
    public void testListTables() {
        tEnv.executeSql("use BSHIS_JXFY");
        tEnv.executeSql("show tables").print();
    }

    @Test
    public void testQuery() {
        //        tEnv.executeSql("use BSHIS_JXFY");
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT cast(WORD as string)"
                                + " FROM oraCatalog.BSHIS2020_EY.ZYYS_EMR_LIST /*+OPTIONS('debezium.log.mining.strategy'='online_catalog',\n"
                                + "                                'scan.incremental.close-idle-reader.enabled'='true',\n"
                                + "\t\t\t\t\t\t\t\t'debezium.lob.enabled'='true')*/");
        tableResult.print();
    }
}
