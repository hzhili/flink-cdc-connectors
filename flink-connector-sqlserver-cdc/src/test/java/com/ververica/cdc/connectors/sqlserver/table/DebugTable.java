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
        env.enableCheckpointing(200);
        env.setParallelism(1);
    }

    @Test
    public void debugTest() throws Throwable {
        String sourceDDL =
                "CREATE TABLE IF NOT EXISTS nRegiPati (\n"
                        + "    `autokh` DECIMAL(18,0)\n"
                        + "    ,`PatiID` DECIMAL(10,0)\n"
                        + "    ,`ybzhcode` DECIMAL(18,0)\n"
                        + "    ,`id_no` STRING\n"
                        + "    ,`gmycode` STRING\n"
                        + "    ,`icdcode` STRING\n"
                        + "    ,`RegiStrDate` STRING\n"
                        + "    ,`crbbtyy` STRING\n"
                        + "    ,`paygroup` DECIMAL(18,0)\n"
                        + "    ,`dbr_name` STRING\n"
                        + "    ,`address` STRING\n"
                        + "    ,`isyy` INT\n"
                        + "    ,`isjzxx` INT\n"
                        + "    ,`MARRIAGE` INT\n"
                        + "    ,`kspdh` STRING\n"
                        + "    ,`pulse_condition` STRING\n"
                        + "    ,`age_unit` STRING\n"
                        + "    ,`RegiID` STRING\n"
                        + "    ,`hzblh` STRING\n"
                        + "    ,`Systolic` DECIMAL(18,0)\n"
                        + "    ,`lxfs` STRING\n"
                        + "    ,`native` STRING\n"
                        + "    ,`sex` INT NOT NULL\n"
                        + "    ,`bl_wwqz` STRING\n"
                        + "    ,`clyj` STRING\n"
                        + "    ,`NATIONALITY` DECIMAL(18,0)\n"
                        + "    ,`hyid` STRING\n"
                        + "    ,`zdstatus` INT\n"
                        + "    ,`bl_xbs` STRING\n"
                        + "    ,`yjs` STRING\n"
                        + "    ,`RegiTrack` INT NOT NULL\n"
                        + "    ,`hzkh` STRING\n"
                        + "    ,`Diastolic` DECIMAL(18,0)\n"
                        + "    ,`pay_channel` INT\n"
                        + "    ,`age` INT\n"
                        + "    ,`age_month_unit` STRING\n"
                        + "    ,`bl_zzzf` STRING\n"
                        + "    ,`PORF` DECIMAL(18,0)\n"
                        + "    ,`price` DECIMAL(12,2)\n"
                        + "    ,`ismzyssfzgh` INT\n"
                        + "    ,`blcode` STRING\n"
                        + "    ,`city` DECIMAL(18,0)\n"
                        + "    ,`bl_jws` STRING\n"
                        + "    ,`CLINRESUNAME` STRING\n"
                        + "    ,`CaseID` STRING\n"
                        + "    ,`bl_jsjc` STRING\n"
                        + "    ,`tongue_nature` STRING\n"
                        + "    ,`gmyname` STRING\n"
                        + "    ,`DoctorId` DECIMAL(18,0)\n"
                        + "    ,`age_month` DECIMAL(18,0)\n"
                        + "    ,`bl_btzz` STRING\n"
                        + "    ,`ISJZ` INT\n"
                        + "    ,`paydate` TIMESTAMP\n"
                        + "    ,`dbr_idno` STRING\n"
                        + "    ,`ZSID` DECIMAL(18,0)\n"
                        + "    ,`Telno` STRING\n"
                        + "    ,`jztime` TIMESTAMP\n"
                        + "    ,`tongue_fur` STRING\n"
                        + "    ,`heihgt` STRING\n"
                        + "    ,`bl_cbyx` STRING\n"
                        + "    ,`fbsj` TIMESTAMP\n"
                        + "    ,`patitypeid` DECIMAL(18,0)\n"
                        + "    ,`doct_order` DECIMAL(18,0)\n"
                        + "    ,`ismzysgh` INT\n"
                        + "    ,`gms` STRING\n"
                        + "    ,`COUNTRY` DECIMAL(18,0)\n"
                        + "    ,`payoperid` DECIMAL(18,0)\n"
                        + "    ,`ishzbr` INT\n"
                        + "    ,`yxjb` INT\n"
                        + "    ,`bl_zs` STRING\n"
                        + "    ,`health_card` STRING\n"
                        + "    ,`zt` INT\n"
                        + "    ,`gmy` DECIMAL(18,0)\n"
                        + "    ,`bl_grs` STRING\n"
                        + "    ,`OfficeID` DECIMAL(18,0)\n"
                        + "    ,`bs_regiid` STRING\n"
                        + "    ,`blzy` STRING\n"
                        + "    ,`status` INT NOT NULL\n"
                        + "    ,`wj_regiid` STRING\n"
                        + "    ,`ZDYJ` STRING\n"
                        + "    ,`village` STRING\n"
                        + "    ,`dzpdh` STRING\n"
                        + "    ,`checkouttype` INT NOT NULL\n"
                        + "    ,`area` DECIMAL(18,0)\n"
                        + "    ,`birthday` TIMESTAMP\n"
                        + "    ,`name` STRING\n"
                        + "    ,`bl_jzs` STRING\n"
                        + "    ,`personal_id` STRING\n"
                        + "    ,`gmymedname` STRING\n"
                        + "    ,`rszt` INT\n"
                        + "    ,`offiid_order` DECIMAL(18,0)\n"
                        + "    ,`lcyx` STRING\n"
                        + "    ,`Parent_lxfs` STRING\n"
                        + "    ,`zjh` STRING\n"
                        + "    ,`BMI` DECIMAL(18,0)\n"
                        + "    ,`fztime` TIMESTAMP\n"
                        + "    ,`province` STRING\n"
                        + "    ,`autonumb` DECIMAL(18,0)\n"
                        + "    ,`hztmh` STRING\n"
                        + "    ,`jkda_dah` STRING\n"
                        + "    ,`bl_fzjc` STRING\n"
                        + "    ,`tradeno` STRING\n"
                        + "    ,`bl_fy` STRING\n"
                        + "    ,`weight` STRING\n"
                        + "    ,`RegiDate` TIMESTAMP\n"
                        + "    ,`y_regiid` STRING\n"
                        + "    ,`operid` DECIMAL(18,0)\n"
                        + "    ,`overmark` INT\n"
                        + "    ,`ckprice` DECIMAL(18,4)\n"
                        + "    ,`CLINRESU` DECIMAL(18,0)\n"
                        + "    ,`bl_tzjc` STRING\n"
                        + "    ,PRIMARY KEY ( `RegiID` ) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "    'connector' = 'sqlserver-cdc',\n"
                        + "    'hostname' = '192.168.0.217',\n"
                        + "    'port' = '1433',\n"
                        + "    'username' = 'sa',\n"
                        + "    'password' = '146-164-152-',\n"
                        + "    'database-name' = 'HISDB',\n"
                        + "    'table-name' = 'DBO.NREGIPATI',\n"
                        + "    'debezium.database.include.list'='HISDB',\n"
                        + "\t'debezium.database.encrypt'='false'\n"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql("select count(1) from nRegiPati").print();
    }
}
