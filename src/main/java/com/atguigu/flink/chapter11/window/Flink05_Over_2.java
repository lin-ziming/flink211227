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
public class Flink05_Over_2 {
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
        tEnv.createTemporaryView("sensor", table);
        
        //        sum(vc) over(partition by id order by et rows between unbounded preceding   and current row)
        
     
       /* tEnv
            .sqlQuery("select " +
                          " id, ts, vc, et, " +
//                          " sum(vc) over(partition by id order by et rows between unbounded preceding and current row) vc_sum " +
//                          " sum(vc) over(partition by id order by et rows between 1 preceding and current row) vc_sum " +
//                          " sum(vc) over(partition by id order by et range between unbounded preceding and current row) vc_sum " +
//                          " sum(vc) over(partition by id order by et range between interval '2' second preceding and current row) vc_sum " +
                          " sum(vc) over(partition by id order by et ) vc_sum " + // 默认是range
                          "from sensor")
            .execute()
            .print();*/
        
        tEnv
            .sqlQuery("select " +
                          " id, ts, vc, et, " +
                          " sum(vc) over w vc_sum, " +
                          " max(vc) over w vc_max " +
                          "from sensor " +
                          "window w as(partition by id order by et rows between unbounded preceding and current row)")
            .execute()
            .print();
        
    }
}
