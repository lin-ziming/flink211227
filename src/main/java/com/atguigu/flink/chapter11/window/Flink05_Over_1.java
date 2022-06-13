package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author lzc
 * @Date 2022/6/11 10:28
 */
public class Flink05_Over_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<WaterSensor> stream =
            env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                              new WaterSensor("sensor_1", 1999L, 20),
                              new WaterSensor("sensor_1", 4000L, 40),
                              new WaterSensor("sensor_1", 4000L, 50),
                              new WaterSensor("sensor_1", 5000L, 50)
                )
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
                );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
//        sum(vc) over(partition by id order by et rows between unbounded preceding   and current row)
//        OverWindow win = Over.partitionBy($("id")).orderBy($("et")).preceding(UNBOUNDED_ROW).following(CURRENT_ROW).as("w");
//        OverWindow win = Over.partitionBy($("id")).orderBy($("et")).preceding(rowInterval(2L)).following(CURRENT_ROW).as("w");
        
//        OverWindow win = Over.partitionBy($("id")).orderBy($("et")).preceding(UNBOUNDED_RANGE).as("w");
        OverWindow win = Over.partitionBy($("id")).orderBy($("et")).preceding(lit(2).second()).as("w");
        
        
        
        table
            .window(win)
            .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")))
            .execute()
            .print();
        
        
        
    }
}
