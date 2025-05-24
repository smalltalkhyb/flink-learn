package com.hyb.flink.sql.services;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;


/**
 * @program: flink-learn
 * @description:
 * @author: huyanbing
 * @create: 2025-05-24
 **/
@Service
public class CdcBaseService {
    public void executeSql(@RequestBody List<String> sqlList) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        sqlList.forEach(sql -> {
            tEnv.executeSql(sql);
        });

    }
}