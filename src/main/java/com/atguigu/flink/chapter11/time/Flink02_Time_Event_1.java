package com.atguigu.flink.chapter11.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneOffset;

/**
 * @Author lzc
 * @Date 2022/6/11 15:04
 */
public class Flink02_Time_Event_1 {
    public static void main(String[] args) {
        // 流转成表的时候定义事件属性
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        
      
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(8));
        // 在ddl中指定时间属性
        tEnv.executeSql("create table sensor(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int, " +
//                            " et as TO_TIMESTAMP_LTZ(ts, 3), " +
                            " et as to_timestamp(from_unixtime(ts/1000)), " +
                            " watermark for et as et - interval '3' second" +
                            ")with(" +
                            " 'connector'='filesystem', " +
                            " 'path'='input/sensor.txt', " +
                            " 'format'='csv' " +
                            ")");
    
        Table table = tEnv.sqlQuery("select * from sensor");
        table.printSchema();
        table.execute().print();
        //bigint -> timestamp(3)   TO_TIMESTAMP_LTZ
        //bigint -> string(yyyy-Mm-dd HH:mm:ss)-> timestamp(3)
    }
}
