package com.hyb.flink.learn.streaming.doris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: spring-learn
 * @description:
 * @author: huyanbing
 * @create: 2025-05-16
 **/
public class FlinkDorisDemo2 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = null;
        // 本机执行环境
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 远程执行环境
        //env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 8081);
        env.enableCheckpointing(3000l).setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String sql = "CREATE TABLE advertising_aggregation (\n" +
                "  `ad_id` INT COMMENT '广告 ID' ,\n" +
                "  `ad_date` DATE COMMENT '广告投放日期' ,\n" +
                "  `impressions` INT COMMENT '展示次数' ,\n" +
                "  `clicks` INT COMMENT '点击次数' ,\n" +
                "  `conversions` INT COMMENT '转化次数' ,\n" +
                "  PRIMARY KEY (ad_id) NOT ENFORCED\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '192.168.1.171:8030',\n" +
                "  'table.identifier' = 'ontology_data_source_test.advertising_aggregation',\n" +
                "  'username' = 'dev_user',\n" +
                "  'password' = 'Dev@02891'\n" +
                ");";

        String targetSql = "CREATE TABLE advertising_aggregation_output (\n" +
                "  `ad_id` INT COMMENT '广告 ID' ,\n" +
                "  `ad_date` DATE COMMENT '广告投放日期' ,\n" +
                "  `impressions` INT COMMENT '展示次数' ,\n" +
                "  `clicks` INT COMMENT '点击次数' ,\n" +
                "  `conversions` INT COMMENT '转化次数' ,\n" +
                "  PRIMARY KEY (ad_id) NOT ENFORCED\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '192.168.1.171:8030',\n" +
                "  'table.identifier' = 'ontology_data_source_test.advertising_aggregation_output',\n" +
                "  'username' = 'dev_user',\n" +
                "  'password' = 'Dev@02891'\n" +
                ");";

        // CREATE TABLE sourceOrderItemDDL
        TableResult tableResult = tableEnv.executeSql(sql);
        System.out.println(tableResult);

        Table table = tableEnv.sqlQuery("select * from advertising_aggregation");

        TableResult targetTableResult = tableEnv.executeSql(targetSql);

        System.out.println(table);

        tableEnv.executeSql(" INSERT INTO `advertising_aggregation_output` (`ad_id`,`ad_date`,`impressions`,`clicks`,`conversions`)  SELECT * FROM `advertising_aggregation` `advertising_aggregation_alias` ");
        //tableEnv.executeSql(" 'INSERT INTO `advertising_aggregation_output` (`ad_id`,`ad_date`,`impressions`,`clicks`,`conversions`) SELECT * FROM (SELECT * FROM `advertising_aggregation` `advertising_aggregation_alias`) `sq0`'");
        Table tableTarget = tableEnv.sqlQuery("select * from advertising_aggregation_output");


    }


}
