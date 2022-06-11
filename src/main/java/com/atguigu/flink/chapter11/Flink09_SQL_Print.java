package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink09_SQL_Print {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 建立一个动态表与file中的文件进行关联
        tEnv.executeSql("create table sensor(" +
                            " id string," +
                            " ts bigint, " +
                            " vc int" +
                            ")with(" +
                            "  'connector' = 'kafka', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'properties.group.id' = 'atguigu', " +
                            "  'topic' = 's1', " +
                            "  'scan.startup.mode' = 'latest-offset', " +
                            "  'format' = 'csv' " +
                            ")");
    
    
        tEnv.executeSql("create table a(" +
                            " id string," +
                            " ts bigint, " +
                            " vc int" +
                            ")with(" +
                            "  'connector' = 'print'" +
                            ")");
    
    
        tEnv.executeSql("create table b(" +
                            " id string" +
                            ")with(" +
                            "  'connector' = 'print'" +
                            ")");
        
        tEnv.sqlQuery("select * from sensor").executeInsert("a");
        
        tEnv.executeSql("insert into b select id from sensor");
    
    }
}
