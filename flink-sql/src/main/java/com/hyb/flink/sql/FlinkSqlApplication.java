package com.hyb.flink.sql;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.locks.LockSupport;

@SpringBootApplication
public class FlinkSqlApplication {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(FlinkSqlApplication.class, args);

        while (true){
            Thread.sleep(30000);
        }
    }

}
