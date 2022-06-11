package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink08_SQL_Kafka_Upsert_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("create table `result`(" +
                            " id string," +
                            " vc_sum int, " +
                            " primary key(id)not enforced " +
                            ")with(" +
                            "  'connector' = 'upsert-kafka', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'topic' = 's3', " +
                            "  'key.format' = 'json' ," +
                            "  'value.format' = 'json' " +
                            ")");
    
    
        tEnv.sqlQuery("select * from `result`").execute().print();
        
        
    
    }
}
