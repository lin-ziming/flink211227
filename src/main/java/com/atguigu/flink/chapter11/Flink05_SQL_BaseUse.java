package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink05_SQL_BaseUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60)
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(stream);
        
        // 1. 查询未注册的表
        //        tEnv.executeSql("ddl 增删改");
        //        tEnv.sqlQuery("查询语句");
    
//        Table result = tEnv.sqlQuery("select * from " + table + " where id='sensor_1'");
//        result.execute().print();
        // 2. 查询已注册的表
        tEnv.createTemporaryView("sensor", table);
        
//        tEnv.sqlQuery("select * from sensor where id='sensor_1'").execute().print();
        tEnv.sqlQuery("select " +
                          " id, " +
                          " sum(vc) sum_vc " +
                          "from sensor " +
                          "group by id")
            .execute()
            .print();
        
    }
}
