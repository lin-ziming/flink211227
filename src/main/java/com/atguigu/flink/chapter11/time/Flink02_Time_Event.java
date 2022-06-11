package com.atguigu.flink.chapter11.time;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneOffset;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/6/11 15:04
 */
public class Flink02_Time_Event {
    public static void main(String[] args) {
        // 流转成表的时候定义事件属性
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<WaterSensor> stream =
            env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                              new WaterSensor("sensor_1", 2000L, 20),
                              new WaterSensor("sensor_2", 3000L, 30),
                              new WaterSensor("sensor_1", 4000L, 40),
                              new WaterSensor("sensor_1", 5000L, 50),
                              new WaterSensor("sensor_2", 6000L, 60)
                )
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
                );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(8));
        
        // 添加时间属性, 表示事件时间
//        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts").rowtime().as("et"), $("vc"));
        
        table.printSchema();
        
        table.execute().print();
    }
}
