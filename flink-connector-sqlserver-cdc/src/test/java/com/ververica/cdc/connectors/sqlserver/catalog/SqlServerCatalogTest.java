package com.ververica.cdc.connectors.sqlserver.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
        tEnv.executeSql("use BSHIS_JXFY");
        tEnv.executeSql("show tables").print();
    }

    @Test
    public void testQuery() {
        tEnv.executeSql("use BSHIS_JXFY");
        tEnv.executeSql(
                        "SELECT * FROM ZYYS_EMR_LIST /*+OPTIONS(\t 'debezium.log.mining.strategy'='online_catalog',\n"
                                + "\t 'debezium.log.mining.continuous.mine'='true',\n"
                                + "\t 'scan.incremental.close-idle-reader.enabled'='true')*/")
                .print();
    }
}
