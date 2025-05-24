package com.hyb.flink.learn.streaming;

import com.hyb.flink.learn.streaming.parameters.MyJobParameters;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: flink-learn
 * @description:
 * @author: huyanbing
 * @create: 2025-05-24
 **/
public class FlinkRemoteStreamExecution {


    public static void main(String[] args) {


        // 参考：https://www.here.com/docs/bundle/data-client-library-developer-guide-java-scala/page/client/flink-connector-migration-guide.html
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL,
                10);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
                Duration.ofMinutes(5));
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY,
                Duration.ofSeconds(10));

        config.setString("exec_sql", "select * from  table1 ");


        List<String> jarList = new ArrayList<>();
        //jarList.add("/usr/local/software/install/flink-1.19.2/examples/streaming/WordCount.jar");
        jarList.add("D:\\workspace-2025-stone-hg\\06\\flink-learn\\flink-lean-table\\target\\flink-lean-table-1.0.0.jar");


        // 解析命令行参数


        // 参考：https://www.cnblogs.com/kunande/p/16353565.html
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                "192.168.1.244",
                8081,
                config,
                jarList.toArray(new String[0]))) {

//            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            env.getConfig().setGlobalJobParameters(new MyJobParameters("param1-----我是参数", "param2--我是参数2"));


            // 2. 定义数据源
            DataStream<String> sourceStream = env.socketTextStream("localhost", 9999);

            sourceStream.print();


            env.execute("FlinkRemoteStreamExecution");

            System.out.println("FlinkRemoteStreamExecution  executed");
        } catch (Exception e) {
            e.printStackTrace();

        } finally {

        }


    }
}
