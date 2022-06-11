package com.atguigu.flink.chapter11.time;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/6/11 15:04
 */
public class Flink01_Time_Processing {
    public static void main(String[] args) {
        // 流转成表的时候定义事件属性
    
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60));
    
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 添加时间属性, 表示处理时间
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"),  $("pt").proctime());
        
        table.printSchema();
        
        table.execute().print();
    }
}
