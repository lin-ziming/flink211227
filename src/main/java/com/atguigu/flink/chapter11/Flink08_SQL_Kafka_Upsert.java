package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink08_SQL_Kafka_Upsert {
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
    
    
        Table result = tEnv.sqlQuery("select id, sum(vc) vc_sum from sensor  group by id");
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
    
    
        
        result.executeInsert("result");
//        tEnv.executeSql("delete from `result` where id='sensor_1'");
    
    
    
    
    }
}
