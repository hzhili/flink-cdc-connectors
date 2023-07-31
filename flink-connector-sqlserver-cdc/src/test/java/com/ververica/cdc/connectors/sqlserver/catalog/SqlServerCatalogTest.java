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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Sql Server Catalog Test. */
@Ignore
public class SqlServerCatalogTest {
    private StreamTableEnvironment tEnv;

    @Before
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        this.tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "create catalog mssqlCatalog with("
                        + " 'type'='sqlserver-ctl',"
                        + " 'hostname' = '192.168.0.217',"
                        + " 'username' = 'sa',"
                        + " 'password' = '146-164-152-',"
                        + " 'database-name' = 'hisdb',"
                        + " 'schema-name' = 'dbo',"
                        + " 'debezium.database.encrypt'='false',"
                        + " 'debezium.database.connectTimeout'='60000'"
                        + ")");
        tEnv.executeSql("use catalog mssqlCatalog");
    }

    @Test
    public void testListDatabases() {
        //        List<String> actual = catalog.listDatabases();
        //        actual.forEach(System.out::println);
        tEnv.executeSql("show databases;").print();
    }

    @Test
    public void testListTables() {
        tEnv.executeSql("use HISDB");
        tEnv.executeSql("show tables").print();
    }

    @Test
    public void testQuery() {
        tEnv.executeSql("use HISDB");
        tEnv.executeSql(
                        "SELECT * FROM NREGIPATI /*+OPTIONS("
                                + "\t 'scan.incremental.close-idle-reader.enabled'='true')*/")
                .print();
    }
}
