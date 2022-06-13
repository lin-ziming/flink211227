package com.atguigu.flink.chapter11.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author lzc
 * @Date 2022/6/13 13:56
 */
public class HiveDemo {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        tEnv.executeSql("create table person(" +
                            " id string," +
                            " ts bigint, " +
                            " vc int" +
                            ")with(" +
                            "   'connector'='filesystem', " +
                            "   'path'='input/sensor.txt', " +
                            "   'format'='csv' " +
                            ")");
        
        // 1. 创建HiveCatalog对象
        HiveCatalog hive = new HiveCatalog("hive", "gmall", "src/main/resources");
        // 2. 注册 HiveCatalog对象
        tEnv.registerCatalog("hive", hive);
        
        tEnv.useCatalog("hive");
        tEnv.useDatabase("gmall");
        
        // 3. 使用HiveCatalog读写hive中的表
        tEnv.sqlQuery("select " +
                          " * " +
                          "from default_catalog.default_database.person")
            .execute()
            .print();
        
    
    
        
    }
}
