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

package com.ververica.cdc.connectors.oracle.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Oracle test. */
public class OracleTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS MZSF_CLININVOINFO_TEMP (\n"
                        + "    `SFID` STRING\n"
                        + "    ,`CLINID` DECIMAL(38,0)\n"
                        + "    ,`CASENO` STRING\n"
                        + "    ,`PAYWAYID` DECIMAL(10,0)\n"
                        + "    ,`ORIGAMOU` DECIMAL(10,2)\n"
                        + "    ,`DISCAMOU` DECIMAL(10,2)\n"
                        + "    ,`OWNPRICE` DECIMAL(12,2)\n"
                        + "    ,`CREADATE` TIMESTAMP\n"
                        + "    ,`CREAOPERID` STRING\n"
                        + "    ,`PAYGROU` DECIMAL(38,0)\n"
                        + "    ,`PAYOPERID` STRING\n"
                        + "    ,`PAYDATE` TIMESTAMP\n"
                        + "    ,`PATITYPEID` STRING\n"
                        + "    ,`STRCREADATE` STRING\n"
                        + "    ,`MZZY` STRING\n"
                        + "    ,`HOSPID` DECIMAL(38,0)\n"
                        + "    ,`SBZFJE` DECIMAL(12,4)\n"
                        + "    ,`SBZFXJJE` DECIMAL(12,4)\n"
                        + "    ,`STATUS` DECIMAL(38,0)\n"
                        + "    ,`ZFFYPRICE` DECIMAL(12,4)\n"
                        + "    ,`GRZFPRICE` DECIMAL(12,4)\n"
                        + "    ,`ABLZFPRICE` DECIMAL(12,4)\n"
                        + "    ,`TCJZPRICE` DECIMAL(12,4)\n"
                        + "    ,`DEJZPRICE` DECIMAL(12,4)\n"
                        + "    ,`GWYJZPRICE` DECIMAL(12,4)\n"
                        + "    ,`GRZHPRICE` DECIMAL(12,4)\n"
                        + "    ,`SBZFPRICE` DECIMAL(12,4)\n"
                        + "    ,`YBZHCODE` STRING\n"
                        + "    ,`HOSPCODE` STRING\n"
                        + "    ,`STATUS_YB` DECIMAL(38,0)\n"
                        + "    ,`FRCODE` STRING\n"
                        + "    ,`FORGID` DECIMAL(8,0)\n"
                        + "    ,`ISUPLOAD` STRING\n"
                        + "    ,`PATID` DECIMAL(38,0)\n"
                        + "    ,`ACCID` DECIMAL(38,0)\n"
                        + "    ,`REGIID` DECIMAL(38,0)\n"
                        + "    ,`CARDID` DECIMAL(38,0)\n"
                        + "    ,`CTIMESTAMP` TIMESTAMP\n"
                        + "    ,`BCDATE` TIMESTAMP\n"
                        + "    ,`BCOPERID` STRING\n"
                        + "    ,`CZYID` DECIMAL(38,0)\n"
                        + "    ,`CZYKSID` DECIMAL(38,0)\n"
                        + "    ,`ZZJID` STRING\n"
                        + "    ,`SCYDD` DECIMAL(38,0)\n"
                        + "    ,`TRADE_NO` STRING\n"
                        + "    ,`TRADE_TYPE` STRING\n"
                        + "    ,`SOURCESTYPE` STRING\n"
                        + "    ,`ISAPPLY` DECIMAL(38,0)\n"
                        + "    ,`APPLYOPERID` STRING\n"
                        + "    ,`SOURCES` STRING\n"
                        + "    ,`UPLOAD_STATUS` STRING\n"
                        + ") WITH (\n"
                        + "     'connector' = 'oracle-cdc',\n"
                        + "     'hostname' = '172.16.89.33',\n"
                        + "     'port' = '1521',\n"
                        + "     'username' = 'cdcuser',\n"
                        + "     'password' = 'cdcuser',\n"
                        + "     'database-name' = 'HISDB',\n"
                        + "     'schema-name' = 'BSHIS60',\n"
                        + "     'table-name' = 'MZSF_CLININVOINFO_TEMP',\n"
                        + "     'scan.incremental.snapshot.chunk.size' = '100000'\n"
                        + ")");
        TableResult tableResult = tEnv.executeSql("SELECT * FROM MZSF_CLININVOINFO_TEMP");
        tableResult.print();
    }
}
