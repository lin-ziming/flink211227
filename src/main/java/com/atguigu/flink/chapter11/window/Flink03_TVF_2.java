package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink03_TVF_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<WaterSensor> stream =
            env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                              new WaterSensor("sensor_1", 2000L, 20),
                              new WaterSensor("sensor_1", 4000L, 40),
                              new WaterSensor("sensor_1", 5000L, 50),
                              new WaterSensor("sensor_1", 17000L, 50),
                              new WaterSensor("sensor_1", 20000L, 50),
                              new WaterSensor("sensor_1", 22000L, 50),
                              new WaterSensor("sensor_1", 26000L, 50)
                )
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
                );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        
        tEnv.createTemporaryView("sensor", table);
        
        tEnv
            .sqlQuery("select " +
                          "id, window_start, window_end, " +
                          " sum(vc) vc_sum " +
                          "from table(  CUMULATE(TABLE sensor, DESCRIPTOR(et), interval '5' second, interval '10' second)  ) " +
                          "group by id, window_start, window_end")
            .execute()
            .print();
       
       
       
        
    }
}
/*
每隔一小时统计一次今天的 pv 值

流:
    定时器
    
    窗口
        状态
        
sql:
    累积窗口



 */