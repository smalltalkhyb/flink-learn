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
public class FlinkDorisDemo {


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


        String sql = "CREATE TABLE cv_branch (\n" +
                "  rid STRING NOT NULL COMMENT '函数唯一ID',\n" +
                "  branch_name STRING NOT NULL COMMENT '分支名称',\n" +
                "  project_rid STRING NOT NULL COMMENT '项目id',\n" +
                "  check_name STRING NOT NULL COMMENT '检查者名称',\n" +
                "  create_uid STRING NOT NULL COMMENT '创建者ID',\n" +
                "  update_uid STRING NOT NULL COMMENT '更新者id',\n" +
                "  create_name STRING NOT NULL COMMENT '创建者名称',\n" +
                "  update_name STRING NOT NULL COMMENT '更新者名称',\n" +
                "  check_uid STRING NOT NULL COMMENT '检查者id',\n" +
                "  merge_status STRING NOT NULL COMMENT '合并状态：0、未合并；1、合并成功；2、合并失败',\n" +
                "  check_status STRING NOT NULL COMMENT '检查状态：0、未检查；1、检查成功；2、检查失败',\n" +
                "  src_branch_name STRING NOT NULL COMMENT '来源分支名称',\n" +
                "  PRIMARY KEY (rid) NOT ENFORCED\n" +
                ")\n" +
                "COMMENT '项目分支表'\n" +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '192.168.1.171:8030',\n" +
                "  'table.identifier' = 'ontology_test_db.cv_branch',\n" +
                "  'username' = 'dev_user',\n" +
                "  'password' = 'Dev@02891'\n" +
                ");\n";

        String targetSql = "CREATE TABLE cv_branch_flink_test (\n" +
                "  rid STRING NOT NULL COMMENT '函数唯一ID',\n" +
                "  branch_name STRING NOT NULL COMMENT '分支名称',\n" +
                "  project_rid STRING NOT NULL COMMENT '项目id',\n" +
                "  check_name STRING NOT NULL COMMENT '检查者名称',\n" +
                "  create_uid STRING NOT NULL COMMENT '创建者ID',\n" +
                "  update_uid STRING NOT NULL COMMENT '更新者id',\n" +
                "  create_name STRING NOT NULL COMMENT '创建者名称',\n" +
                "  update_name STRING NOT NULL COMMENT '更新者名称',\n" +
                "  check_uid STRING NOT NULL COMMENT '检查者id',\n" +
                "  merge_status STRING NOT NULL COMMENT '合并状态：0、未合并；1、合并成功；2、合并失败',\n" +
                "  check_status STRING NOT NULL COMMENT '检查状态：0、未检查；1、检查成功；2、检查失败',\n" +
                "  src_branch_name STRING NOT NULL COMMENT '来源分支名称',\n" +
                "  PRIMARY KEY (rid) NOT ENFORCED\n" +
                ")\n" +
                "COMMENT '项目分支表'\n" +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '192.168.1.171:8030',\n" +
                "  'table.identifier' = 'ontology_test_db.cv_branch_flink_test',\n" +
                "  'username' = 'dev_user',\n" +
                "  'password' = 'Dev@02891'\n" +
                ");\n";

        // CREATE TABLE sourceOrderItemDDL
        TableResult tableResult = tableEnv.executeSql(sql);
        System.out.println(tableResult);

        Table table = tableEnv.sqlQuery("select * from cv_branch");

        TableResult targetTableResult = tableEnv.executeSql(targetSql);

        System.out.println(table);

        tableEnv.executeSql("insert into  cv_branch_flink_test  (select * from cv_branch)  ");

        Table tableTarget = tableEnv.sqlQuery("select * from cv_branch_flink_test");


    }


}
