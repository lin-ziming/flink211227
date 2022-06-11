package com.atguigu.flink.chapter11.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/11 15:04
 */
public class Flink01_Time_Processing_1 {
    public static void main(String[] args) {
        // 流转成表的时候定义事件属性
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 在ddl中指定时间属性
        tEnv.executeSql("create table sensor(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int, " +
                            " pt as proctime()" +
                            ")with(" +
                            " 'connector'='filesystem', " +
                            " 'path'='input/sensor.txt', " +
                            " 'format'='csv' " +
                            ")");
        
        tEnv.sqlQuery("select * from sensor").execute().print();
    }
}
