package com.hyb.flink.learn.streaming.parameters;

import org.apache.flink.api.common.ExecutionConfig;

/**
 * @program: flink-learn
 * @description:
 * @author: huyanbing
 * @create: 2025-05-23
 **/
public class MyJobParameters extends ExecutionConfig.GlobalJobParameters {
    private String param1;
    private String param2;

    public MyJobParameters(String param1, String param2) {
        this.param1 = param1;
        this.param2 = param2;
    }

    @Override
    public String toString() {
        return "MyJobParameters{" +
                "param1='" + param1 + '\'' +
                ", param2='" + param2 + '\'' +
                '}';
    }
}